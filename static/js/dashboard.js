// dashboard.js
let active_tf = '15m'; 
let global_data = null;
let ws = null;

function connectWebSocket() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws_url = protocol + "//" + window.location.host + "/ws";
    
    ws = new WebSocket(ws_url);

    ws.onmessage = function(event) {
        const data = JSON.parse(event.data);
        updateDashboard(data);
    };

    ws.onclose = function() {
        console.log("WebSocket connection lost. Reconnecting in 5 seconds...");
        setTimeout(connectWebSocket, 5000);
    };

    ws.onerror = function(err) {
        ws.close();
    };
}

// ট্যাব চেঞ্জ বা নতুন টাইমফ্রেম সিলেক্ট করার লজিক
function selectTF(tf) {
    active_tf = tf;
    should_fit_content = true; // নতুন টাইমফ্রেম সিলেক্ট করলে চার্ট একবার স্বয়ংক্রিয়ভাবে জুম ফিট হবে
    renderActiveTF();
    loadCandles(); 
}

function renderActiveTF() {
    if (!global_data) return;
    const d = global_data;
    
    const tfs = ['15m', '30m', '45m', '1h', '2h', '3h', '4h', '1d'];
    tfs.forEach(t => {
        const btn = document.getElementById('btn_' + t);
        if (!btn) return;
        if (t === active_tf) {
            btn.className = "px-3 py-1.5 rounded-lg font-black text-[11px] bg-blue-600 text-white shadow-sm border border-blue-600 flex-shrink-0 transition-all";
        } else {
            btn.className = "px-3 py-1.5 rounded-lg font-bold text-[11px] bg-slate-50 text-slate-500 hover:bg-slate-100 border border-slate-200 flex-shrink-0 transition-all";
        }
    });

    const tfKey = 'analysis_' + active_tf;
    const tfData = d[tfKey] || { rsi: 0, ema20: 0, ema50: 0, sig: 'লোড হচ্ছে...', pats: [] };

    document.getElementById('tf_rsi').innerText = tfData.rsi;
    document.getElementById('tf_e20').innerText = '$' + tfData.ema20;
    document.getElementById('tf_e50').innerText = '$' + tfData.ema50;

    const vwapRow = document.getElementById('tf_vwap_row');
    if (active_tf === '15m' && tfData.vwap) {
        vwapRow.classList.remove('hidden');
        document.getElementById('tf_vwap').innerText = '$' + tfData.vwap;
    } else {
        vwapRow.classList.add('hidden');
    }

    const h1ExtraRow = document.getElementById('tf_h1_extra');
    if (active_tf === '1h' && tfData.ema200) {
        h1ExtraRow.classList.remove('hidden');
        document.getElementById('tf_e200').innerText = '$' + d.analysis_1h.ema200;
        document.getElementById('tf_bp').innerText = '$' + d.analysis_1h.btc_price;
    } else {
        h1ExtraRow.classList.add('hidden');
    }

    const sigBadge = document.getElementById('tf_sig');
    sigBadge.innerText = tfData.sig;
    if (tfData.sig.includes('বুলিশ')) {
        sigBadge.className = 'font-bold px-2 py-0.5 rounded text-[10px] bg-green-50 text-green-700 border border-green-200';
    } else if (tfData.sig.includes('বেয়ারিশ')) {
        sigBadge.className = 'font-bold px-2 py-0.5 rounded text-[10px] bg-red-50 text-red-700 border border-red-200';
    } else {
        sigBadge.className = 'font-bold px-2 py-0.5 rounded text-[10px] bg-slate-100 text-slate-600 border border-slate-200';
    }

    const tag = (p) => {
        let tagClass = 'tag-neutral';
        if (p.t === 'bull') tagClass = 'tag-bull';
        if (p.t === 'bear') tagClass = 'tag-bear';
        return `<span class="tag ${tagClass}">${p.n}</span>`;
    };
    const no_pat = '<p class="text-gray-400 italic text-[10px]">কোনো ক্যান্ডেলস্টিক প্যাটার্ন নেই</p>';
    document.getElementById('tf_pats').innerHTML = tfData.pats.length > 0 ? tfData.pats.map(tag).join('') : no_pat;
}

function renderCheckItem(label, is_passed) {
    const icon = is_passed ? '✅' : '❌';
    const color = is_passed ? 'text-slate-800' : 'text-slate-400 font-normal';
    return `<div class="flex items-center gap-1.5 ${color}"><span>${icon}</span><span>${label}</span></div>`;
}

function updateDashboard(d) {
    if (!d || d.price <= 0) return;
    
    global_data = d; 
    
    document.getElementById('pr').innerText = '$' + d.price; 
    document.getElementById('bl').innerText = '$' + d.balance.toFixed(2);
    
    document.getElementById('t').innerText = d.trades; 
    document.getElementById('w').innerText = d.win_rate + '%';
    document.getElementById('pnl').innerText = (d.total_pnl >= 0 ? '+$' : '$') + d.total_pnl.toFixed(2);
    document.getElementById('bt').innerText = '$' + d.best.toFixed(2); 
    document.getElementById('wt').innerText = '$' + d.worst.toFixed(2);
    document.getElementById('la').innerText = d.last_action; 
    document.getElementById('st').innerText = '⌛ ' + d.wait_reason;
    document.getElementById('est').innerText = d.estimated_time;
    
    document.getElementById('pdh').innerText = '$' + d.pdh.toFixed(2);
    document.getElementById('pdl').innerText = '$' + d.pdl.toFixed(2);
    document.getElementById('h4_res').innerText = '$' + d.h4_res.toFixed(2);
    document.getElementById('h4_sup').innerText = '$' + d.h4_sup.toFixed(2);
    
    updateLiveChartPrice(d.price);

    const exitPanel = document.getElementById('exit_checklist_panel');
    if (d.in_position) {
        exitPanel.classList.remove('hidden');
        const disp = document.getElementById('pnl_display'); 
        disp.classList.remove('hidden');
        
        document.getElementById('lp').innerText = (d.live_pnl_pct >= 0 ? '+' : '') + d.live_pnl_pct + '%';
        document.getElementById('sl').innerText = d.sl_level; 
        document.getElementById('tp').innerText = d.tp_level;
        
        const col = d.live_pnl_pct >= 0 ? 'text-green-600' : 'text-red-600';
        document.getElementById('lp').className = 'text-4xl font-black ' + col;
        disp.className = 'mb-4 p-5 border-2 rounded-3xl text-center bg-white shadow-lg ' + (d.live_pnl_pct >= 0 ? 'border-green-100' : 'border-red-100');
        
        const p_type = document.getElementById('pos_type');
        p_type.innerText = d.position_type;
        
        const exit_mode = document.getElementById('exit_mode');
        const exit_container = document.getElementById('exit_checklist');
        let exit_html = '';
        
        if (d.position_type === 'LONG') {
            p_type.className = 'text-[10px] font-black px-2 py-0.5 rounded bg-green-50 text-green-700 border border-green-200 uppercase';
            exit_mode.innerText = 'LONG EXIT MONITOR 🟢';
            exit_mode.className = 'text-[9px] px-2 py-0.5 rounded font-black bg-green-50 text-green-700 border border-green-100';
            exit_html += renderCheckItem('স্টপ লস সীমা সুরক্ষিত', d.exit_conditions.sl_safe);
            exit_html += renderCheckItem('টেক প্রফিট লক্ষ্যের নিচে', d.exit_conditions.tp_safe);
            exit_html += renderCheckItem('১৫মি ইএমএ ৫০ ট্রেন্ড নিরাপদ', d.exit_conditions.ema50_safe);
            exit_html += renderCheckItem('আরএসআই এক্সট্রিম জোন নিরাপদ', d.exit_conditions.rsi_safe);
            exit_html += renderCheckItem('আসল পুঁজি ব্রেক-ইভেনে সুরক্ষিত', d.exit_conditions.is_breakeven);
        } else if (d.position_type === 'SHORT') {
            p_type.className = 'text-[10px] font-black px-2 py-0.5 rounded bg-red-50 text-red-700 border border-red-200 uppercase';
            exit_mode.innerText = 'SHORT EXIT MONITOR 🔴';
            exit_mode.className = 'text-[9px] px-2 py-0.5 rounded font-black bg-red-50 text-red-700 border border-red-100';
            exit_html += renderCheckItem('স্টপ লস সীমা সুরক্ষিত', d.exit_conditions.sl_safe);
            exit_html += renderCheckItem('টেক প্রফিট লক্ষ্যের ওপরে', d.exit_conditions.tp_safe);
            exit_html += renderCheckItem('১৫মি ইএমএ ৫০ ট্রেন্ড নিরাপদ', d.exit_conditions.ema50_safe);
            exit_html += renderCheckItem('আরএসআই এক্সট্রিম জোন নিরাপদ', d.exit_conditions.rsi_safe);
            exit_html += renderCheckItem('আসল পুঁজি ব্রেক-ইভেনে সুরক্ষিত', d.exit_conditions.is_breakeven);
        } else {
            p_type.className = 'hidden';
        }
        exit_container.innerHTML = exit_html;
    } else { 
        exitPanel.classList.add('hidden');
        document.getElementById('pnl_display').classList.add('hidden'); 
    }
    
    const long_container = document.getElementById('long_checklist');
    const short_container = document.getElementById('short_checklist');
    const conf = d.confluences;
    
    let long_html = '';
    long_html += renderCheckItem('১ঘণ্টা ম্যাক্রো আপট্রেন্ড', conf.macro_bullish);
    long_html += renderCheckItem('বিটকয়েন ট্রেন্ড আপ', conf.btc_bullish);
    long_html += renderCheckItem('মূল্য VWAP এর ওপরে', conf.vwap_long);
    long_html += renderCheckItem('১৫মি ইএমএ এলাইনমেন্ট', conf.ema_long);
    long_html += renderCheckItem('১ঘণ্টা ম্যাকডি বুলিশ', conf.macd_long);
    long_html += renderCheckItem('ভলিউম ব্রেকআউট কনফার্ম', conf.volume_confirmed);
    long_html += renderCheckItem('সবুজ ক্যান্ডেল প্যাটার্ন', conf.bull_signal);
    long_container.innerHTML = long_html;
    
    let short_html = '';
    short_html += renderCheckItem('১ঘণ্টা ম্যাক্রো ডাউনট্রেন্ড', conf.macro_bearish);
    short_html += renderCheckItem('বিটকয়েন ট্রেন্ড ডাউন', conf.btc_bearish);
    short_html += renderCheckItem('মূল্য VWAP এর নিচে', conf.vwap_short);
    short_html += renderCheckItem('১৫মি ইএমএ ডাউন-এলাইন', conf.ema_short);
    short_html += renderCheckItem('১ঘণ্টা ম্যাকডি বেয়ারিশ', conf.macd_short);
    short_html += renderCheckItem('ভলিউম ব্রেকআউটের কনফার্ম', conf.volume_confirmed);
    short_html += renderCheckItem('লাল ক্যান্ডেল প্যাটার্ন', conf.bear_signal);
    short_container.innerHTML = short_html;
    
    renderActiveTF();
    updateMarkersAndLines(active_tf_res, active_tf_sup, active_tf_name);

    document.getElementById('hb').innerHTML = d.history.slice(0,5).map(h => `
        <tr class="border-b border-slate-50">
            <td class="py-2 text-slate-400 font-bold">${h.t}</td>
            <td class="font-black text-center ${h.a=='BUY'?'text-blue-500':'text-orange-500'}">${h.a}</td>
            <td class="text-right font-black">$${h.p}</td>
            <td class="text-right font-black ${h.r.includes('-')?'text-red-400':'text-green-500'}">${h.r}</td>
        </tr>
    `).join('');
    
    document.getElementById('lg').innerHTML = d.log.slice(0,3).map(l => `
        <div class="flex justify-between text-slate-500 pb-1">
            <span>${l.t}</span>
            <span>${l.m}</span>
        </div>
    `).join('');
}

window.onload = function() {
    initChart();
    connectWebSocket();
    setInterval(loadCandles, 15000);
};
