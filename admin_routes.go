package main

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const queuePagePath = "/admin/virtual-library"

type adminRoutes struct {
	pb.UnimplementedHttpRoutesServer
	server *runtimeServer
}

type prowlarrReleaseResult struct {
	Title       string `json:"title"`
	Indexer     string `json:"indexer"`
	Size        int64  `json:"size"`
	IMDbID      int64  `json:"imdb_id,omitempty"`
	TMDBID      int64  `json:"tmdb_id,omitempty"`
	TVDBID      int64  `json:"tvdb_id,omitempty"`
	PublishDate string `json:"publish_date,omitempty"`
}

func newAdminRoutes(server *runtimeServer) *adminRoutes { return &adminRoutes{server: server} }

// sensitiveQueryParamPattern strips credential-bearing query parameters so
// upstream transport errors (which embed full URLs) can be shown to admins
// without disclosing API keys or tokens.
var sensitiveQueryParamPattern = regexp.MustCompile(`(?i)([?&](?:api_?key|apikey|token|secret|password|access_?token)=)[^&\s'"]+`)

func redactError(err error) string {
	if err == nil {
		return ""
	}
	return sensitiveQueryParamPattern.ReplaceAllString(err.Error(), "${1}[redacted]")
}

func (a *adminRoutes) Handle(ctx context.Context, req *pb.HandleHTTPRequest) (*pb.HandleHTTPResponse, error) {
	if req == nil || !strings.EqualFold(req.GetHeaders()["X-Silo-User-Role"], "admin") {
		return adminJSON(http.StatusForbidden, map[string]string{"error": "admin access required"})
	}
	path := strings.TrimRight(req.GetPath(), "/")
	switch {
	case path == queuePagePath && req.GetMethod() == http.MethodGet:
		var body bytes.Buffer
		if err := queuePage.Execute(&body, nil); err != nil {
			return nil, err
		}
		return adminHTML(http.StatusOK, body.String())
	case path == queuePagePath+"/queue" && req.GetMethod() == http.MethodGet:
		return a.queueJSON()
	case path == queuePagePath+"/calendar" && req.GetMethod() == http.MethodGet:
		return a.calendarJSON()
	case (path == queuePagePath+"/schedule/refresh" || path == "/admin/schedule/refresh") && req.GetMethod() == http.MethodPost:
		return a.refreshSchedule(ctx)
	case (path == queuePagePath+"/schedule" || path == "/admin/schedule") && req.GetMethod() == http.MethodGet:
		return a.scheduleJSON()
	case path == queuePagePath+"/queue/force" && req.GetMethod() == http.MethodPost:
		return a.forceQueueItem(ctx, req.GetQuery().GetFields())
	case path == queuePagePath+"/queue/search" && req.GetMethod() == http.MethodPost:
		return a.searchQueueItem(ctx, req.GetQuery().GetFields())
	case path == queuePagePath+"/queue/remove" && req.GetMethod() == http.MethodPost:
		return a.removeQueueItem(req.GetQuery().GetFields())
	default:
		return adminJSON(http.StatusNotFound, map[string]string{"error": "route not found"})
	}
}

func (a *adminRoutes) searchQueueItem(ctx context.Context, fields map[string]*structpb.Value) (*pb.HandleHTTPResponse, error) {
	key := queryString(fields, "key")
	item, ok := a.server.monitor.item(key)
	if !ok {
		return adminJSON(http.StatusNotFound, map[string]string{"error": "queue item not found"})
	}
	a.server.monitor.mu.Lock()
	client := a.server.monitor.prowlarr
	quality := a.server.monitor.config.Quality
	a.server.monitor.mu.Unlock()
	if client == nil || client.URL() == "" {
		return adminJSON(http.StatusBadRequest, map[string]string{"error": "Prowlarr is not configured"})
	}
	releases, err := client.SearchItem(ctx, item)
	if err != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(err)})
	}
	matched := matchProwlarrReleasesWithQuality(releases, item, quality)
	if matched {
		item.Ready = true
		item.Force = false
		if err := a.server.monitor.register(ctx, item); err != nil {
			return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(err)})
		}
		a.server.monitor.markRegistered(item.Key)
		if err := a.server.monitor.remember(item); err != nil {
			return adminJSON(http.StatusInternalServerError, map[string]string{"error": redactError(err)})
		}
	}
	results := make([]prowlarrReleaseResult, 0, len(releases))
	for _, release := range releases {
		results = append(results, prowlarrReleaseResult{Title: release.Title, Indexer: release.Indexer, Size: release.Size, IMDbID: release.IMDbID, TMDBID: release.TMDBID, TVDBID: release.TVDBID, PublishDate: release.PublishDate})
	}
	return adminJSON(http.StatusOK, map[string]any{"matched": matched, "registered": matched, "title": item.Title, "count": len(results), "releases": results})
}

func (a *adminRoutes) queueJSON() (*pb.HandleHTTPResponse, error) {
	a.server.monitor.mu.Lock()
	items := make([]monitoredMedia, 0, len(a.server.monitor.items))
	now := time.Now()
	for _, item := range a.server.monitor.items {
		if item.MediaType == "series" {
			item.Episodes = missingEpisodes(item.Episodes, now)
			if len(item.Episodes) > 0 {
				items = append(items, item)
			}
			continue
		}
		if !item.Ready {
			items = append(items, item)
		}
	}
	a.server.monitor.mu.Unlock()
	sort.Slice(items, func(i, j int) bool {
		return strings.ToLower(items[i].Title) < strings.ToLower(items[j].Title)
	})
	return adminJSON(http.StatusOK, map[string]any{"items": items, "count": len(items), "generated_at": time.Now().UTC()})
}

func (a *adminRoutes) calendarJSON() (*pb.HandleHTTPResponse, error) {
	a.server.monitor.mu.Lock()
	items := make([]monitoredMedia, 0, len(a.server.monitor.items))
	for _, item := range a.server.monitor.items {
		if !item.Ready && !item.Release.IsZero() {
			items = append(items, item)
		}
	}
	a.server.monitor.mu.Unlock()
	sort.Slice(items, func(i, j int) bool { return items[i].Release.Before(items[j].Release) })
	return adminJSON(http.StatusOK, map[string]any{"items": items, "count": len(items), "from": time.Now().UTC().Format("2006-01-02"), "to": time.Now().UTC().AddDate(0, 1, 0).Format("2006-01-02")})
}

func (a *adminRoutes) scheduleJSON() (*pb.HandleHTTPResponse, error) {
	if a.server == nil || a.server.releaseStore == nil {
		return adminJSON(http.StatusOK, map[string]any{
			"count":        0,
			"shows":        []any{},
			"generated_at": time.Now().UTC(),
		})
	}
	return adminJSON(http.StatusOK, a.server.releaseStore.GetScheduleSummary())
}

func (a *adminRoutes) refreshSchedule(ctx context.Context) (*pb.HandleHTTPResponse, error) {
	if a.server == nil || a.server.scheduler == nil || a.server.releaseStore == nil {
		return adminJSON(http.StatusBadRequest, map[string]string{"error": "release scheduler is not configured"})
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := a.server.scheduler.RefreshAll(refreshCtx); err != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(err)})
	}
	summary := a.server.releaseStore.GetScheduleSummary()
	return adminJSON(http.StatusOK, map[string]any{
		"message": "Schedule refresh completed",
		"summary": summary,
	})
}

func (a *adminRoutes) forceQueueItem(ctx context.Context, fields map[string]*structpb.Value) (*pb.HandleHTTPResponse, error) {
	key := queryString(fields, "key")
	item, ok := a.server.monitor.item(key)
	if !ok {
		return adminJSON(http.StatusNotFound, map[string]string{"error": "queue item not found"})
	}
	item.Force = true
	updated, message, evaluationErr := a.server.monitor.evaluate(ctx, item)
	if evaluationErr != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(evaluationErr)})
	}
	if !updated.Ready || strings.TrimSpace(updated.Title) == "" || (updated.MediaType == "series" && len(updated.Episodes) == 0) {
		return adminJSON(http.StatusConflict, map[string]string{"error": "metadata is not ready to register", "message": message})
	}
	if err := a.server.monitor.register(ctx, updated); err != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(err)})
	}
	a.server.monitor.markRegistered(updated.Key)
	if err := a.server.monitor.remember(updated); err != nil {
		return adminJSON(http.StatusInternalServerError, map[string]string{"error": redactError(err)})
	}
	return adminJSON(http.StatusOK, map[string]any{"item": updated, "message": "Media was force-added to the virtual library"})
}

func (a *adminRoutes) removeQueueItem(fields map[string]*structpb.Value) (*pb.HandleHTTPResponse, error) {
	key := queryString(fields, "key")
	if key == "" {
		return adminJSON(http.StatusBadRequest, map[string]string{"error": "key is required"})
	}
	if err := a.server.monitor.forget(key); err != nil {
		return adminJSON(http.StatusInternalServerError, map[string]string{"error": redactError(err)})
	}
	return adminJSON(http.StatusOK, map[string]string{"message": "Queue item removed"})
}

func queryString(fields map[string]*structpb.Value, key string) string {
	if value, ok := fields[key]; ok {
		return strings.TrimSpace(value.GetStringValue())
	}
	return ""
}

func adminJSON(status int, payload any) (*pb.HandleHTTPResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &pb.HandleHTTPResponse{StatusCode: int32(status), Body: body, Headers: map[string]string{"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store"}}, nil
}

func adminHTML(status int, body string) (*pb.HandleHTTPResponse, error) {
	return &pb.HandleHTTPResponse{StatusCode: int32(status), Body: []byte(body), Headers: map[string]string{"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store"}}, nil
}

var queuePage = template.Must(template.New("queue").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Virtual Library | Release Desk</title><style>
:root{color-scheme:dark;--bg:#090b10;--panel:#11151d;--panel2:#171d27;--text:#f4f0e8;--muted:#8e98a7;--line:#2a3340;--accent:#e7b86b;--accent2:#9fd5c1;--warn:#ee8f78}*{box-sizing:border-box}body{margin:0;background:radial-gradient(ellipse at 80% -10%,#3a3020 0,transparent 38%),radial-gradient(ellipse at 0 30%,#18242a 0,transparent 32%),var(--bg);color:var(--text);font:15px/1.5 Inter,ui-sans-serif,system-ui,sans-serif}main{max-width:1240px;margin:0 auto;padding:44px 26px 72px}.eyebrow{text-transform:uppercase;letter-spacing:.16em;color:var(--accent);font-size:10px;font-weight:900}.hero{display:flex;justify-content:space-between;gap:20px;align-items:end;margin:8px 0 30px}.hero h1{font-size:clamp(36px,6vw,68px);line-height:.95;margin:0;letter-spacing:-.07em}.hero p{color:var(--muted);max-width:620px;margin:14px 0 0}.hero-actions{display:flex;gap:9px;flex-wrap:wrap}.refresh,.back{border:0;border-radius:9px;padding:10px 15px;font-weight:800;cursor:pointer;text-decoration:none}.back{background:transparent;border:1px solid var(--line);color:var(--text)}.refresh{background:var(--accent);border:1px solid var(--accent);border-radius:9px;color:#171008;padding:11px 17px;font-weight:800;cursor:pointer}.summary{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--line);border:1px solid var(--line);margin-bottom:34px}.stat,.card{background:linear-gradient(110deg,var(--panel2),var(--panel));border:1px solid var(--line)}.stat{background:var(--panel);padding:19px 21px}.stat strong{display:block;font-size:31px;letter-spacing:-.04em}.stat span{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.12em}.toolbar{display:flex;justify-content:space-between;gap:12px;align-items:center;margin:24px 0 12px}.toolbar h2{font-size:18px;margin:0;letter-spacing:-.02em}.toolbar small{color:var(--muted)}#queue{display:grid;gap:9px}.card{padding:16px 18px;display:flex;justify-content:space-between;gap:20px;align-items:center}.card.pending{border-left:4px solid var(--warn)}.card.ready{border-left:4px solid var(--accent)}.title{font-size:18px;font-weight:800}.meta{display:flex;flex-wrap:wrap;gap:7px;margin-top:7px}.chip{border:1px solid var(--line);border-radius:999px;color:var(--muted);font-size:11px;padding:3px 8px}.chip.force{color:var(--accent);border-color:#347c70}.chip.movie{color:#ffaaa8}.chip.series{color:#9bc9ff}.message{color:var(--muted);font-size:13px;margin-top:10px}.actions{display:flex;gap:8px;flex-shrink:0}.actions button{border:1px solid var(--line);background:#202d40;color:var(--text);border-radius:10px;padding:9px 12px;cursor:pointer;font-weight:700}.actions button.primary{background:var(--accent);border-color:var(--accent);color:#071410}.empty{padding:50px;text-align:center;border:1px dashed var(--line);border-radius:0;color:var(--muted)}@media(max-width:700px){main{padding:30px 14px}.hero{align-items:start;flex-direction:column}.summary{grid-template-columns:1fr 1fr}.card{align-items:start;flex-direction:column}.actions{width:100%}.actions button{flex:1}}
</style><style>.calendar-card{display:grid;grid-template-columns:72px 1fr auto;align-items:center;gap:16px;background:linear-gradient(145deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:18px;padding:10px 14px}.calendar-date{display:grid;place-items:center;border-right:1px solid var(--line);padding-right:16px;color:var(--accent);font-weight:800;text-align:center}.calendar-date strong{font-size:22px;line-height:1}.calendar-date span{font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:var(--muted)}@media(max-width:700px){.calendar-card{grid-template-columns:58px 1fr}.calendar-card .chip{grid-column:2;justify-self:start}.calendar-date{padding-right:10px}}#toasts{position:fixed;right:18px;bottom:18px;display:grid;gap:8px;z-index:50;max-width:min(420px,90vw)}.toast{background:var(--panel2);border:1px solid var(--line);border-left:3px solid var(--accent2);padding:11px 15px;border-radius:10px;font-size:13px;line-height:1.45;box-shadow:0 6px 24px rgba(0,0,0,.35);transition:opacity .3s ease;word-break:break-word}.toast.error{border-left-color:var(--warn)}.toast.gone{opacity:0}button:focus-visible,a:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:9px}button:disabled{opacity:.55;cursor:wait}
</style></head><body><main><div class="eyebrow">Silo Virtual Library</div><div class="hero"><div><h1>Release calendar</h1><p>Review monitored media as it moves from upcoming to available. The Prowlarr index refreshes on schedule without searching every title.</p></div><div class="hero-actions"><a class="back" href="/admin/plugins">&#8592; Back to Admin</a><button class="refresh" type="button" aria-label="Refresh TVmaze release schedules for tracked shows" onclick="refreshSchedules(this)">Refresh schedules</button><button type="button" aria-label="Reload monitored media and calendar" onclick="load()">Refresh page data</button></div></div><section class="summary"><div class="stat"><strong id="total">—</strong><span>Monitored items</span></div><div class="stat"><strong id="pending">—</strong><span>Waiting for release</span></div><div class="stat"><strong id="forced">—</strong><span>Force-added</span></div></section><div class="toolbar"><h2>Monitored media</h2><small id="updated">Loading…</small></div><section id="queue"></section><div class="toolbar"><h2>Upcoming episodes</h2><small id="scheduleUpdated">Tracked series air dates</small></div><section id="schedule"></section><div class="toolbar"><h2>Upcoming releases</h2><small>Next 30 days</small></div><section id="calendar"></section></main><script>
const esc=s=>String(s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function toast(message,kind){let host=document.querySelector('#toasts');if(!host){host=document.createElement('div');host.id='toasts';host.setAttribute('role','status');host.setAttribute('aria-live','polite');document.body.appendChild(host)}const el=document.createElement('div');el.className='toast'+(kind?' '+kind:'');el.textContent=String(message||'');host.appendChild(el);setTimeout(()=>{el.classList.add('gone');setTimeout(()=>el.remove(),350)},4500)}
const root=location.pathname.replace(/\/$/,'');
const keyOf=i=>encodeURIComponent(i||'');
async function action(path,key,btn){if(btn){if(btn.disabled)return false;btn.disabled=true;setTimeout(()=>{try{btn.disabled=false}catch(e){}},1500)}let d={};try{const r=await fetch(root+'/queue/'+path+'?key='+keyOf(key),{method:'POST'});d=await r.json();if(!r.ok)toast(d.error||d.message||'Request failed','error')}catch(err){toast('Network error while contacting Silo','error')}await load();return d}
async function searchRelease(key,btn){const d=await action('search',key,btn);if(!d||!d.releases)return;if(!d.releases.length){toast('No releases returned for '+(d.title||key),'error');return}const top=d.releases.slice(0,2).map(r=>r.title).join(' | ');toast((d.matched?'Release found for '+(d.title||''):'No exact release match for '+(d.title||''))+' — '+d.releases.length+' result(s): '+top,'info')}
async function refreshSchedules(btn){if(!btn||btn.disabled)return;btn.disabled=true;const original=btn.textContent;btn.textContent='Refreshing…';try{const r=await fetch(root+'/schedule/refresh',{method:'POST'});const d=await r.json();if(!r.ok){toast(d.error||'Schedule refresh failed','error')}else{const c=d.summary&&d.summary.count!=null?d.summary.count:'?';toast('Schedules refreshed — '+c+' show(s) tracked','info')}}catch(err){toast('Network error during schedule refresh','error')}finally{btn.textContent=original;btn.disabled=false}await load()}
function card(i){const type=i.media_type==='movie'?'Movie':'Series';const state=i.force?'Force-added':'Waiting';const missing=i.media_type==='series'&&i.episodes?.length?i.episodes.map(e=>'S'+String(e.season).padStart(2,'0')+'E'+String(e.episode).padStart(2,'0')+(e.title?' '+e.title:'')).join(', '):'';const eps=missing?(i.episodes.length+' missing episode'+(i.episodes.length===1?'':'s')):'';let html='<article class="card pending"><div><div class="title">'+esc(i.title||i.key)+'</div><div class="meta"><span class="chip '+esc(i.media_type)+'">'+type+'</span><span class="chip">'+esc(state)+'</span>';if(eps)html+='<span class="chip">'+esc(eps)+'</span>';if(i.year)html+='<span class="chip">'+esc(i.year)+'</span>';if(i.force)html+='<span class="chip force">Manual override</span>';html+='</div><div class="message">'+(missing?'Missing: '+esc(missing):'Release verification is still pending.')+'</div></div><div class="actions">';if(!i.force)html+='<button class="primary" type="button" onclick="searchRelease('+JSON.stringify(i.key)+',this)">Request release</button><button class="primary" type="button" aria-label="Force add '+esc(i.title||i.key)+'" onclick="action(\'force\','+JSON.stringify(i.key)+',this)">Force add</button>';html+='<button type="button" aria-label="Remove '+esc(i.title||i.key)+' from queue" onclick="if(confirm(\'Remove this item from the monitor queue?\'))action(\'remove\','+JSON.stringify(i.key)+',this)">Remove</button></div></article>';return html}
 function calendarCard(i){const when=new Date(i.release);const day=when.toLocaleDateString(undefined,{day:'2-digit'});const month=when.toLocaleDateString(undefined,{month:'short'});return '<article class="calendar-card"><div class="calendar-date"><strong>'+day+'</strong><span>'+month+'</span></div><div><div class="title">'+esc(i.title||i.key)+'</div><div class="message">Home-media release gate is being monitored.</div></div><span class="chip">'+esc(i.media_type)+'</span></article>'}
function scheduleCard(s){const eps=Object.values(s.episodes||{}).filter(e=>e&&e.air_date&&!isNaN(new Date(e.air_date))).sort((a,b)=>new Date(a.air_date)-new Date(b.air_date));const upcoming=eps.filter(e=>new Date(e.air_date)>new Date()).slice(0,3);const when=upcoming.length?new Date(upcoming[0].air_date):null;const day=when?when.toLocaleDateString(undefined,{day:'2-digit'}):'&#8212;';const month=when?when.toLocaleDateString(undefined,{month:'short'}):'';const list=upcoming.map(e=>'S'+String(e.season).padStart(2,'0')+'E'+String(e.episode).padStart(2,'0')+' &#183; '+new Date(e.air_date).toLocaleString()).join('<br>');return '<article class="calendar-card"><div class="calendar-date"><strong>'+day+'</strong><span>'+month+'</span></div><div><div class="title">'+esc(s.title||s.imdb_id)+'</div><div class="message">'+(list?('Next: '+list):'No future episodes tracked; ended or between seasons.')+'</div></div><span class="chip">'+esc(s.status||'Unknown')+'</span></article>'}
async function load(){const box=document.querySelector('#queue');try{const [queueResponse,calendarResponse,scheduleResponse]=await Promise.all([fetch(root+'/queue'),fetch(root+'/calendar'),fetch(root+'/schedule')]);const d=await queueResponse.json();const calendar=await calendarResponse.json();const schedule=await scheduleResponse.json().catch(()=>({shows:[]}));const items=d.items||[];document.querySelector('#total').textContent=items.length;document.querySelector('#pending').textContent=items.filter(i=>!i.ready).length;document.querySelector('#forced').textContent=items.filter(i=>i.force).length;document.querySelector('#updated').textContent='Updated '+new Date().toLocaleTimeString();box.innerHTML=items.length?items.map(card).join(''):'<div class="empty">The monitor queue is clear.</div>';const shows=schedule.shows||[];document.querySelector('#schedule').innerHTML=shows.length?shows.map(scheduleCard).join(''):'<div class="empty">No series schedules tracked yet.</div>';document.querySelector('#scheduleUpdated').textContent=(schedule.count||shows.length)+' show(s) tracked';document.querySelector('#calendar').innerHTML=calendar.items?.length?calendar.items.map(calendarCard).join(''):'<div class="empty">No upcoming release dates are currently known.</div>'}catch(e){box.innerHTML='<div class="empty">Queue unavailable. Try refreshing.</div>';document.querySelector('#calendar').innerHTML='<div class="empty">Calendar unavailable.</div>';document.querySelector('#schedule').innerHTML='<div class="empty">Schedule unavailable.</div>'}}
load();</script></body></html>`))
