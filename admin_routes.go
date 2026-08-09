package main

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"sort"
	"strings"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const queuePagePath = "/admin/virtual-library"

type adminRoutes struct {
	pb.UnimplementedHttpRoutesServer
	server *runtimeServer
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
	case path == queuePagePath+"/queue/force" && req.GetMethod() == http.MethodPost:
		return a.forceQueueItem(ctx, req.GetQuery().GetFields())
	case path == queuePagePath+"/queue/remove" && req.GetMethod() == http.MethodPost:
		return a.removeQueueItem(req.GetQuery().GetFields())
	default:
		return adminJSON(http.StatusNotFound, map[string]string{"error": "route not found"})
	}
}

func (a *adminRoutes) queueJSON() (*pb.HandleHTTPResponse, error) {
	a.server.monitor.mu.Lock()
	items := make([]monitoredMedia, 0, len(a.server.monitor.items))
	for _, item := range a.server.monitor.items {
		items = append(items, item)
	}
	a.server.monitor.mu.Unlock()
	sort.Slice(items, func(i, j int) bool {
		if items[i].Ready != items[j].Ready {
			return !items[i].Ready
		}
		return strings.ToLower(items[i].Title) < strings.ToLower(items[j].Title)
	})
	return adminJSON(http.StatusOK, map[string]any{"items": items, "count": len(items)})
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
<title>Virtual Library Queue</title><style>
:root{color-scheme:dark;--bg:#0b1018;--panel:#131b27;--panel2:#192435;--text:#edf3fb;--muted:#91a0b4;--line:#28374a;--accent:#77d6c0;--warn:#f5bd68}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 15% 0,#203c4c 0,transparent 34%),var(--bg);color:var(--text);font:15px/1.5 Inter,ui-sans-serif,system-ui,sans-serif}main{max-width:1180px;margin:0 auto;padding:48px 22px 64px}.eyebrow{text-transform:uppercase;letter-spacing:.16em;color:var(--accent);font-size:11px;font-weight:800}.hero{display:flex;justify-content:space-between;gap:20px;align-items:end;margin:8px 0 30px}.hero h1{font-size:clamp(30px,5vw,52px);line-height:1.02;margin:0;letter-spacing:-.04em}.hero p{color:var(--muted);max-width:620px;margin:14px 0 0}.hero-actions{display:flex;gap:9px;flex-wrap:wrap}.refresh,.back{border:0;border-radius:999px;padding:11px 17px;font-weight:800;cursor:pointer;text-decoration:none}.back{background:#202d40;border:1px solid var(--line);color:var(--text)}.refresh{background:var(--accent);border:0;border-radius:999px;color:#071410;padding:11px 17px;font-weight:800;cursor:pointer}.summary{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:22px}.stat,.card{background:linear-gradient(145deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:18px}.stat{padding:17px 19px}.stat strong{display:block;font-size:27px}.stat span{color:var(--muted);font-size:12px}.toolbar{display:flex;justify-content:space-between;gap:12px;align-items:center;margin:24px 0 12px}.toolbar h2{font-size:18px;margin:0}.toolbar small{color:var(--muted)}#queue{display:grid;gap:12px}.card{padding:18px;display:flex;justify-content:space-between;gap:20px;align-items:center}.card.pending{border-left:4px solid var(--warn)}.card.ready{border-left:4px solid var(--accent)}.title{font-size:18px;font-weight:800}.meta{display:flex;flex-wrap:wrap;gap:7px;margin-top:7px}.chip{border:1px solid var(--line);border-radius:999px;color:var(--muted);font-size:11px;padding:3px 8px}.chip.force{color:var(--accent);border-color:#347c70}.chip.movie{color:#ffaaa8}.chip.series{color:#9bc9ff}.message{color:var(--muted);font-size:13px;margin-top:10px}.actions{display:flex;gap:8px;flex-shrink:0}.actions button{border:1px solid var(--line);background:#202d40;color:var(--text);border-radius:10px;padding:9px 12px;cursor:pointer;font-weight:700}.actions button.primary{background:var(--accent);border-color:var(--accent);color:#071410}.empty{padding:50px;text-align:center;border:1px dashed var(--line);border-radius:18px;color:var(--muted)}@media(max-width:700px){main{padding:30px 14px}.hero{align-items:start;flex-direction:column}.summary{grid-template-columns:1fr 1fr}.card{align-items:start;flex-direction:column}.actions{width:100%}.actions button{flex:1}}
</style></head><body><main><div class="eyebrow">Silo Virtual Library</div><div class="hero"><div><h1>Queue control</h1><p>Review media waiting on release metadata. Force-add a false positive when you know the title is available, or remove an item from monitoring.</p></div><div class="hero-actions"><a class="back" href="/admin/plugins">&#8592; Back to Admin</a><button class="refresh" onclick="load()">Refresh queue</button></div></div><section class="summary"><div class="stat"><strong id="total">—</strong><span>Monitored items</span></div><div class="stat"><strong id="pending">—</strong><span>Waiting for release</span></div><div class="stat"><strong id="forced">—</strong><span>Force-added</span></div></section><div class="toolbar"><h2>Monitored media</h2><small id="updated">Loading…</small></div><section id="queue"></section></main><script>
const esc=s=>String(s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const root=location.pathname.replace(/\/$/,'');
const keyOf=i=>encodeURIComponent(i||'');
async function action(path,key){const r=await fetch(root+'/queue/'+path+'?key='+keyOf(key),{method:'POST'});const d=await r.json();if(!r.ok)alert(d.error||d.message||'Request failed');await load()}
function card(i){const type=i.media_type==='movie'?'Movie':'Series';const state=i.force?'Force-added':(i.ready?'Ready':'Waiting');const eps=i.episodes&&i.episodes.length?(i.episodes.length+' aired episode'+(i.episodes.length===1?'':'s')):'';let html='<article class="card '+(i.ready?'ready':'pending')+'"><div><div class="title">'+esc(i.title||i.key)+'</div><div class="meta"><span class="chip '+esc(i.media_type)+'">'+type+'</span><span class="chip">'+esc(state)+'</span>';if(eps)html+='<span class="chip">'+esc(eps)+'</span>';if(i.year)html+='<span class="chip">'+esc(i.year)+'</span>';if(i.force)html+='<span class="chip force">Manual override</span>';html+='</div><div class="message">'+(i.ready?'Registered in the virtual library.':'Release or air-date verification is still pending.')+'</div></div><div class="actions">';if(!i.force&&!i.ready)html+='<button class="primary" onclick="action(\'force\','+JSON.stringify(i.key)+')">Force add</button>';html+='<button onclick="if(confirm(\'Remove this item from the monitor queue?\'))action(\'remove\','+JSON.stringify(i.key)+')">Remove</button></div></article>';return html}
async function load(){const box=document.querySelector('#queue');try{const r=await fetch(root+'/queue');const d=await r.json();const items=d.items||[];document.querySelector('#total').textContent=items.length;document.querySelector('#pending').textContent=items.filter(i=>!i.ready).length;document.querySelector('#forced').textContent=items.filter(i=>i.force).length;document.querySelector('#updated').textContent='Updated '+new Date().toLocaleTimeString();box.innerHTML=items.length?items.map(card).join(''):'<div class="empty">The monitor queue is clear.</div>'}catch(e){box.innerHTML='<div class="empty">Queue unavailable. Try refreshing.</div>'}}
load();</script></body></html>`))
