package main

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"net/http"
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
		return adminJSON(http.StatusBadGateway, map[string]string{"error": err.Error()})
	}
	matched := matchProwlarrReleasesWithQuality(releases, item, quality)
	if matched {
		item.Ready = true
		item.Force = false
		if err := a.server.monitor.register(ctx, item); err != nil {
			return adminJSON(http.StatusBadGateway, map[string]string{"error": err.Error()})
		}
		a.server.monitor.markRegistered(item.Key)
		if err := a.server.monitor.remember(item); err != nil {
			return adminJSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
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
	for _, item := range a.server.monitor.items {
		if !item.Ready {
			items = append(items, item)
		}
	}
	a.server.monitor.mu.Unlock()
	sort.Slice(items, func(i, j int) bool {
		if items[i].Ready != items[j].Ready {
			return !items[i].Ready
		}
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

func (a *adminRoutes) forceQueueItem(ctx context.Context, fields map[string]*structpb.Value) (*pb.HandleHTTPResponse, error) {
	key := queryString(fields, "key")
	item, ok := a.server.monitor.item(key)
	if !ok {
		return adminJSON(http.StatusNotFound, map[string]string{"error": "queue item not found"})
	}
	item.Force = true
	updated, message, evaluationErr := a.server.monitor.evaluate(ctx, item)
	if evaluationErr != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": evaluationErr.Error()})
	}
	if !updated.Ready || strings.TrimSpace(updated.Title) == "" || (updated.MediaType == "series" && len(updated.Episodes) == 0) {
		return adminJSON(http.StatusConflict, map[string]string{"error": "metadata is not ready to register", "message": message})
	}
	if err := a.server.monitor.register(ctx, updated); err != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": err.Error()})
	}
	a.server.monitor.markRegistered(updated.Key)
	if err := a.server.monitor.remember(updated); err != nil {
		return adminJSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return adminJSON(http.StatusOK, map[string]any{"item": updated, "message": "Media was force-added to the virtual library"})
}

func (a *adminRoutes) removeQueueItem(fields map[string]*structpb.Value) (*pb.HandleHTTPResponse, error) {
	key := queryString(fields, "key")
	if key == "" {
		return adminJSON(http.StatusBadRequest, map[string]string{"error": "key is required"})
	}
	if err := a.server.monitor.forget(key); err != nil {
		return adminJSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
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
<title>Virtual Library Calendar</title><style>
:root{color-scheme:dark;--bg:#0b1018;--panel:#131b27;--panel2:#192435;--text:#edf3fb;--muted:#91a0b4;--line:#28374a;--accent:#77d6c0;--warn:#f5bd68}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 15% 0,#203c4c 0,transparent 34%),var(--bg);color:var(--text);font:15px/1.5 Inter,ui-sans-serif,system-ui,sans-serif}main{max-width:1180px;margin:0 auto;padding:48px 22px 64px}.eyebrow{text-transform:uppercase;letter-spacing:.16em;color:var(--accent);font-size:11px;font-weight:800}.hero{display:flex;justify-content:space-between;gap:20px;align-items:end;margin:8px 0 30px}.hero h1{font-size:clamp(30px,5vw,52px);line-height:1.02;margin:0;letter-spacing:-.04em}.hero p{color:var(--muted);max-width:620px;margin:14px 0 0}.hero-actions{display:flex;gap:9px;flex-wrap:wrap}.refresh,.back{border:0;border-radius:999px;padding:11px 17px;font-weight:800;cursor:pointer;text-decoration:none}.back{background:#202d40;border:1px solid var(--line);color:var(--text)}.refresh{background:var(--accent);border:0;border-radius:999px;color:#071410;padding:11px 17px;font-weight:800;cursor:pointer}.summary{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:22px}.stat,.card{background:linear-gradient(145deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:18px}.stat{padding:17px 19px}.stat strong{display:block;font-size:27px}.stat span{color:var(--muted);font-size:12px}.toolbar{display:flex;justify-content:space-between;gap:12px;align-items:center;margin:24px 0 12px}.toolbar h2{font-size:18px;margin:0}.toolbar small{color:var(--muted)}#queue{display:grid;gap:12px}.card{padding:18px;display:flex;justify-content:space-between;gap:20px;align-items:center}.card.pending{border-left:4px solid var(--warn)}.card.ready{border-left:4px solid var(--accent)}.title{font-size:18px;font-weight:800}.meta{display:flex;flex-wrap:wrap;gap:7px;margin-top:7px}.chip{border:1px solid var(--line);border-radius:999px;color:var(--muted);font-size:11px;padding:3px 8px}.chip.force{color:var(--accent);border-color:#347c70}.chip.movie{color:#ffaaa8}.chip.series{color:#9bc9ff}.message{color:var(--muted);font-size:13px;margin-top:10px}.actions{display:flex;gap:8px;flex-shrink:0}.actions button{border:1px solid var(--line);background:#202d40;color:var(--text);border-radius:10px;padding:9px 12px;cursor:pointer;font-weight:700}.actions button.primary{background:var(--accent);border-color:var(--accent);color:#071410}.empty{padding:50px;text-align:center;border:1px dashed var(--line);border-radius:18px;color:var(--muted)}@media(max-width:700px){main{padding:30px 14px}.hero{align-items:start;flex-direction:column}.summary{grid-template-columns:1fr 1fr}.card{align-items:start;flex-direction:column}.actions{width:100%}.actions button{flex:1}}
</style><style>.calendar-card{display:grid;grid-template-columns:72px 1fr auto;align-items:center;gap:16px;background:linear-gradient(145deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:18px;padding:10px 14px}.calendar-date{display:grid;place-items:center;border-right:1px solid var(--line);padding-right:16px;color:var(--accent);font-weight:800;text-align:center}.calendar-date strong{font-size:22px;line-height:1}.calendar-date span{font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:var(--muted)}@media(max-width:700px){.calendar-card{grid-template-columns:58px 1fr}.calendar-card .chip{grid-column:2;justify-self:start}.calendar-date{padding-right:10px}}
</style></head><body><main><div class="eyebrow">Silo Virtual Library</div><div class="hero"><div><h1>Release calendar</h1><p>Review monitored media as it moves from upcoming to available. The Prowlarr index refreshes on schedule without searching every title.</p></div><div class="hero-actions"><a class="back" href="/admin/plugins">&#8592; Back to Admin</a><button class="refresh" onclick="load()">Refresh calendar</button></div></div><section class="summary"><div class="stat"><strong id="total">—</strong><span>Monitored items</span></div><div class="stat"><strong id="pending">—</strong><span>Waiting for release</span></div><div class="stat"><strong id="forced">—</strong><span>Force-added</span></div></section><div class="toolbar"><h2>Monitored media</h2><small id="updated">Loading…</small></div><section id="queue"></section><div class="toolbar"><h2>Upcoming releases</h2><small>Next 30 days</small></div><section id="calendar"></section></main><script>
const esc=s=>String(s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const root=location.pathname.replace(/\/$/,'');
const keyOf=i=>encodeURIComponent(i||'');
async function action(path,key){const r=await fetch(root+'/queue/'+path+'?key='+keyOf(key),{method:'POST'});const d=await r.json();if(!r.ok)alert(d.error||d.message||'Request failed');await load();return d}
async function searchRelease(key){const d=await action('search',key);if(!d||!d.releases)return;const lines=d.releases.slice(0,12).map(r=>r.title+' ['+(r.indexer||'unknown')+']').join('\n');alert((d.matched?'Release found':'No exact release match')+' for '+d.title+'\n\n'+(lines||'No releases returned.'))}
function card(i){const type=i.media_type==='movie'?'Movie':'Series';const state=i.force?'Force-added':'Waiting';const eps=i.episodes&&i.episodes.length?(i.episodes.length+' aired episode'+(i.episodes.length===1?'':'s')):'';let html='<article class="card pending"><div><div class="title">'+esc(i.title||i.key)+'</div><div class="meta"><span class="chip '+esc(i.media_type)+'">'+type+'</span><span class="chip">'+esc(state)+'</span>';if(eps)html+='<span class="chip">'+esc(eps)+'</span>';if(i.year)html+='<span class="chip">'+esc(i.year)+'</span>';if(i.force)html+='<span class="chip force">Manual override</span>';html+='</div><div class="message">Release or air-date verification is still pending.</div></div><div class="actions">';if(!i.force)html+='<button class="primary" onclick="searchRelease('+JSON.stringify(i.key)+')">Request release</button><button class="primary" onclick="action(\'force\','+JSON.stringify(i.key)+')">Force add</button>';html+='<button onclick="if(confirm(\'Remove this item from the monitor queue?\'))action(\'remove\','+JSON.stringify(i.key)+')">Remove</button></div></article>';return html}
 function calendarCard(i){const when=new Date(i.release);const day=when.toLocaleDateString(undefined,{day:'2-digit'});const month=when.toLocaleDateString(undefined,{month:'short'});return '<article class="calendar-card"><div class="calendar-date"><strong>'+day+'</strong><span>'+month+'</span></div><div><div class="title">'+esc(i.title||i.key)+'</div><div class="message">Home-media release gate is being monitored.</div></div><span class="chip">'+esc(i.media_type)+'</span></article>'}
async function load(){const box=document.querySelector('#queue');try{const [queueResponse,calendarResponse]=await Promise.all([fetch(root+'/queue'),fetch(root+'/calendar')]);const d=await queueResponse.json();const calendar=await calendarResponse.json();const items=d.items||[];document.querySelector('#total').textContent=items.length;document.querySelector('#pending').textContent=items.filter(i=>!i.ready).length;document.querySelector('#forced').textContent=items.filter(i=>i.force).length;document.querySelector('#updated').textContent='Updated '+new Date().toLocaleTimeString();box.innerHTML=items.length?items.map(card).join(''):'<div class="empty">The monitor queue is clear.</div>';document.querySelector('#calendar').innerHTML=calendar.items?.length?calendar.items.map(calendarCard).join(''):'<div class="empty">No upcoming release dates are currently known.</div>'}catch(e){box.innerHTML='<div class="empty">Queue unavailable. Try refreshing.</div>';document.querySelector('#calendar').innerHTML='<div class="empty">Calendar unavailable.</div>'}}
load();</script></body></html>`))
