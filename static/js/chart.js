// chart.js
let chart = null;
let candleSeries = null;
let active_price_lines = []; 

let active_tf_res = 0;
let active_tf_sup = 0;
let active_tf_name = '';
let last_candle = null; 
let should_fit_content = true; // জুম ট্র্যাকিং গ্লোবাল ফ্ল্যাগ

function initChart() {
    const container = document.getElementById('lightweight_chart');
    if (!container) return;

    container.style.width = '100%';
    container.style.height = '100%';

    const width = container.clientWidth || window.innerWidth || 340;

    chart = LightweightCharts.createChart(container, {
        width: width,
        height: 256,
        layout: {
            background: { type: 'solid', color: '#ffffff' },
            textColor: '#64748b',
            fontFamily: 'Segoe UI, sans-serif',
            attributionLogo: false
        },
        grid: {
            vertLines: { color: '#f8fafc' },
            horzLines: { color: '#f8fafc' },
        },
        rightPriceScale: {
            borderColor: '#f1f5f9',
        },
        timeScale: {
            borderColor: '#f1f5f9',
            timeVisible: true,
            secondsVisible: false,
        },
    });

    candleSeries = chart.addCandlestickSeries({
        upColor: '#22c55e',
        downColor: '#ef4444',
        borderVisible: false,
        wickUpColor: '#22c55e',
        wickDownColor: '#ef4444',
    });

    window.addEventListener('resize', () => {
        chart.resize(container.clientWidth || window.innerWidth || 340, 256);
    });

    loadCandles();

    setTimeout(() => {
        chart.resize(container.clientWidth || window.innerWidth || 340, 256);
        chart.timeScale().fitContent();
    }, 100);
}

async function loadCandles() {
    try {
        const r = await fetch('/api/ohlcv?tf=' + active_tf);
        const res = await r.json();
        const data = res.candles;
        if (data && data.length > 0) {
            candleSeries.setData(data);
            
            // শুধুমাত্র ফার্স্ট লোড বা টাইমফ্রেম চেঞ্জের সময় জুম রিসেট বা ফিট হবে
            if (should_fit_content) {
                chart.timeScale().fitContent();
                should_fit_content = false; // অটো-রিসেট বন্ধ করার জন্য লক করে দেওয়া হলো
            }
            
            last_candle = { ...data[data.length - 1] };
            active_tf_res = res.tf_res;
            active_tf_sup = res.tf_sup;
            active_tf_name = res.tf_name;

            if (global_data) {
                updateMarkersAndLines(active_tf_res, active_tf_sup, active_tf_name);
            }
        }
    } catch (e) {
        console.log("Error loading candles:", e);
    }
}

// প্রতি মিলি-সেকেন্ডে আসা প্রাইস আপডেট চার্টের ক্যান্ডেল-এ লাইভ পুশ করা (জুম পজিশন না হারিয়ে)
function updateLiveChartPrice(price) {
    if (!candleSeries || !last_candle) return;
    
    const tf_seconds_map = {
        '15m': 900, '30m': 1800, '45m': 2700, '1h': 3600, '2h': 7200, '3h': 10800, '4h': 14400, '1d': 86400
    };
    const interval_sec = tf_seconds_map[active_tf] || 900;
    const now = Math.floor(Date.now() / 1000);
    const current_candle_time = Math.floor(now / interval_sec) * interval_sec;

    if (current_candle_time > last_candle.time) {
        last_candle = {
            time: current_candle_time,
            open: price,
            high: price,
            low: price,
            close: price
        };
    } else {
        last_candle.close = price;
        if (price > last_candle.high) last_candle.high = price;
        if (price < last_candle.low) last_candle.low = price;
    }
    candleSeries.update(last_candle);
}

// রিসেট বাটনে চাপলে চার্টটি স্বয়ংক্রিয়ভাবে জুম রিকভার বা ফিট হবে
function resetChartZoom() {
    if (chart) {
        chart.timeScale().fitContent();
    }
}

function updateMarkersAndLines(tf_res, tf_sup, tf_name) {
    if (!global_data || !candleSeries) return;
    const d = global_data;

    active_price_lines.forEach(line => candleSeries.removePriceLine(line));
    active_price_lines = [];

    // S&R লাইন্স
    if (tf_res > 0) {
        active_price_lines.push(candleSeries.createPriceLine({
            price: tf_res, color: '#ef4444', lineWidth: 1.5, lineStyle: 1, axisLabelVisible: true, title: tf_name + ' Res'
        }));
    }
    if (tf_sup > 0) {
        active_price_lines.push(candleSeries.createPriceLine({
            price: tf_sup, color: '#10b981', lineWidth: 1.5, lineStyle: 1, axisLabelVisible: true, title: tf_name + ' Sup'
        }));
    }

    // লাইভ অর্ডার লেভেল
    if (d.in_position) {
        if (d.entry_price > 0) {
            active_price_lines.push(candleSeries.createPriceLine({
                price: d.entry_price, color: '#3b82f6', lineWidth: 1.5, lineStyle: 0, axisLabelVisible: true, title: 'ENTRY'
            }));
        }
        if (d.sl_level > 0) {
            active_price_lines.push(candleSeries.createPriceLine({
                price: d.sl_level, color: '#b91c1c', lineWidth: 1.5, lineStyle: 0, axisLabelVisible: true, title: 'SL 🛑'
            }));
        }
        if (d.tp_level > 0) {
            active_price_lines.push(candleSeries.createPriceLine({
                price: d.tp_level, color: '#047857', lineWidth: 1.5, lineStyle: 0, axisLabelVisible: true, title: 'TP ✅'
            }));
        }
    }

    // চার্ট মার্কার
    if (d.history && d.history.length > 0) {
        const markers = [];
        const sortedHistory = [...d.history].sort((a, b) => a.ts - b.ts);
        
        const tf_seconds_map = {
            '15m': 900, '30m': 1800, '45m': 2700, '1h': 3600, '2h': 7200, '3h': 10800, '4h': 14400, '1d': 86400
        };
        const interval_sec = tf_seconds_map[active_tf] || 900;

        sortedHistory.forEach(h => {
            if (!h.ts) return;
            const candle_ts = Math.floor(h.ts / interval_sec) * interval_sec;
            const is_buy = h.a === 'BUY';
            
            markers.push({
                time: candle_ts,
                position: is_buy ? 'belowBar' : 'aboveBar',
                color: is_buy ? '#3b82f6' : '#f97316',
                shape: is_buy ? 'arrowUp' : 'arrowDown',
                text: is_buy ? 'BUY' : 'SELL',
                size: 1
            });
        });
        candleSeries.setMarkers(markers);
    }
}
