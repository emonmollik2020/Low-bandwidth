# ai_engine.py (রিগ্রেশন-ভিত্তিক এআই ইঞ্জিন — Walk-Forward ভ্যালিডেশনসহ)
# BANDWIDTH OPTIMIZATION:
#   - save_model_to_db() COMMENTED OUT (no cloud save)
#   - pickle.dump() COMMENTED OUT (no local file save)
#   - load_model_from_db() COMMENTED OUT (no cloud load)
#   - load_or_train_ai_model() — fresh train only, no loading (RAM only)
#   - Training logic UNCHANGED (walk-forward, 15m trigger, same as before)
#
# আগে এটা ৩-ক্লাস ক্লাসিফায়ার ছিল (1/-1/0) যেখানে LONG/SHORT লেবেল খুবই বিরল ছিল — ফলে
# বাস্তবে শেখার মতো নমুনা হাতে গোনা থাকত। এখন RandomForestRegressor প্রতিটা ক্যান্ডেলের
# জন্য পরবর্তী ১ ঘণ্টার প্রকৃত % রিটার্ন প্রেডিক্ট করে (make_regression_target) — পুরো
# ডাটাসেটই কাজে লাগে।
#
# ফিক্সড: আগে মাত্র একটামাত্র ৮৫/১৫ ট্রেন/টেস্ট স্প্লিট দিয়ে ভ্যালিডেট করা হতো — এতে একবারের
# ভাগ্যে (বা দুর্ভাগ্যে) মডেল পাস/ফেল করত, যেটা প্রকৃত ধারাবাহিক এজের প্রমাণ না। এখন
# Walk-Forward ভ্যালিডেশন ব্যবহার হচ্ছে — একাধিক সময়ক্রম-ভিত্তিক (এক্সপ্যান্ডিং উইন্ডো) ফোল্ডে
# আলাদা আলাদা মডেল ট্রেইন-টেস্ট করে দেখা হয় এজটা ধারাবাহিকভাবে বিভিন্ন সময়কালে টিকে থাকে কিনা,
# শুধু একটা নির্দিষ্ট সময়ে ভাগ্যক্রমে ভালো ফল না।
import os
import pickle
import numpy as np
import pandas as pd
import asyncio
from sklearn.ensemble import RandomForestRegressor
from config import MODEL_FILE, DB_ENABLED, state_manager, live_bus
# BANDWIDTH OPT: Commented out DB model load/save
# from database import load_model_from_db, save_model_to_db
import features

ai_model = None  # dict বান্ডল: {"model", "threshold_long", "threshold_short", "n_features"}

N_FOLDS = 5
MIN_SIGNAL_COUNT = 25          # সবগুলো ফোল্ড মিলিয়ে অন্তত এতগুলো সিগন্যাল লাগবে
MIN_SIGNAL_PRECISION = 0.55    # সবগুলো ফোল্ড মিলিয়ে অন্তত এই precision লাগবে
MIN_FOLD_PASS_RATIO = 0.6      # যথেষ্ট সিগন্যাল-থাকা ফোল্ডগুলোর অন্তত ৬০%-এ precision >=৫০% (কয়েন-ফ্লিপের চেয়ে ভালো) হতে হবে
MIN_SIGNALS_PER_FOLD_FOR_CHECK = 3  # এর কম সিগন্যাল থাকা ফোল্ড কনসিস্টেন্সি-চেক থেকে বাদ (পরিসংখ্যানগতভাবে অর্থহীন)
SIGNAL_PERCENTILE = 0.10


def _evaluate_fold(X_tr, y_tr, X_te, y_te):
    """একটা ফোল্ডে মডেল ট্রেইন করে টেস্ট সেটে সিগন্যাল-প্রিসিশন হিসাব করে"""
    thr_long = float(y_tr.quantile(1 - SIGNAL_PERCENTILE))
    thr_short = float(y_tr.quantile(SIGNAL_PERCENTILE))

    m = RandomForestRegressor(n_estimators=100, max_depth=8, random_state=42, n_jobs=-1)
    m.fit(X_tr, y_tr)
    preds = m.predict(X_te)
    y_te_vals = y_te.values

    long_sig = preds >= thr_long
    short_sig = preds <= thr_short
    n_long = int(long_sig.sum())
    n_short = int(short_sig.sum())
    n_sig = n_long + n_short
    n_correct = int((y_te_vals[long_sig] > 0).sum()) + int((y_te_vals[short_sig] < 0).sum())

    return {
        "n_signals": n_sig, "n_long": n_long, "n_short": n_short,
        "n_correct": n_correct,
        "precision": (n_correct / n_sig) if n_sig > 0 else None
    }


def _walk_forward_validate(X, y, n_folds=N_FOLDS):
    """
    সময়ক্রম-ভিত্তিক এক্সপ্যান্ডিং-উইন্ডো ফোল্ড তৈরি করে প্রতিটাতে আলাদা মডেল ট্রেইন-টেস্ট করে।
    ফোল্ড ১: প্রথম ১/(n+1) অংশ দিয়ে ট্রেইন, পরের অংশ দিয়ে টেস্ট।
    ফোল্ড ২: প্রথম ২/(n+1) অংশ দিয়ে ট্রেইন (আগের টেস্ট অংশও এখন ট্রেনিংয়ে), পরের অংশ টেস্ট। ইত্যাদি।
    """
    n = len(X)
    chunk = n // (n_folds + 1)
    fold_results = []

    for i in range(n_folds):
        train_end = chunk * (i + 1)
        test_end = chunk * (i + 2) if i < n_folds - 1 else n

        if train_end < 200 or (test_end - train_end) < 30:
            continue

        X_tr, y_tr = X.iloc[:train_end], y.iloc[:train_end]
        X_te, y_te = X.iloc[train_end:test_end], y.iloc[train_end:test_end]

        result = _evaluate_fold(X_tr, y_tr, X_te, y_te)
        result["fold"] = i + 1
        result["train_rows"] = train_end
        result["test_rows"] = test_end - train_end
        fold_results.append(result)

    return fold_results


def train_model_in_background(df, current_btc_price=None):
    try:
        global ai_model
        print("[AI Training] Preparing multi-timeframe features (regression) in background...", flush=True)

        df_train = df.copy().sort_values('t').reset_index(drop=True)
        if len(df_train) < 800:
            print("[AI Training] Not enough candle data for walk-forward validation, skipping training.", flush=True)
            return

        df_train = features.build_features(df_train)
        df_train['target'] = features.make_regression_target(df_train)
        df_train = df_train.dropna(subset=['target']).reset_index(drop=True)

        X = df_train[features.FEATURE_COLS].fillna(0)
        y = df_train['target']

        print(f"[AI Training] {len(X)} সারিতে {N_FOLDS}-ফোল্ড Walk-Forward ভ্যালিডেশন চালানো হচ্ছে...", flush=True)
        fold_results = _walk_forward_validate(X, y, N_FOLDS)

        if not fold_results:
            print("[AI Training] Walk-forward ফোল্ড তৈরি করার মতো যথেষ্ট ডাটা নেই, স্কিপ করা হলো।", flush=True)
            return

        for r in fold_results:
            prec_str = f"{r['precision']:.2%}" if r['precision'] is not None else "N/A"
            print(f"    ফোল্ড {r['fold']}: ট্রেইন {r['train_rows']} | টেস্ট {r['test_rows']} | সিগন্যাল {r['n_signals']} (L:{r['n_long']}/S:{r['n_short']}) | প্রিসিশন {prec_str}", flush=True)

        total_signals = sum(r["n_signals"] for r in fold_results)
        total_correct = sum(r["n_correct"] for r in fold_results)
        overall_precision = (total_correct / total_signals) if total_signals > 0 else 0.0

        # কনসিস্টেন্সি-চেক: যথেষ্ট সিগন্যাল থাকা ফোল্ডগুলোর মধ্যে কতগুলো কয়েন-ফ্লিপের (৫০%) চেয়ে ভালো করেছে
        checkable_folds = [r for r in fold_results if r["n_signals"] >= MIN_SIGNALS_PER_FOLD_FOR_CHECK]
        passing_folds = [r for r in checkable_folds if r["precision"] is not None and r["precision"] >= 0.50]
        fold_pass_ratio = (len(passing_folds) / len(checkable_folds)) if checkable_folds else 0.0

        print(
            f"[AI Validation] মোট সিগন্যাল: {total_signals} | সার্বিক প্রিসিশন: {overall_precision:.2%} | "
            f"ফোল্ড কনসিস্টেন্সি: {len(passing_folds)}/{len(checkable_folds)} ({fold_pass_ratio:.0%})",
            flush=True
        )

        deployed = (
            total_signals >= MIN_SIGNAL_COUNT and
            overall_precision >= MIN_SIGNAL_PRECISION and
            len(checkable_folds) >= 3 and
            fold_pass_ratio >= MIN_FOLD_PASS_RATIO
        )

        state_manager.update({
            "ai_validation": {
                "n_signals": total_signals,
                "signal_precision": round(overall_precision * 100, 1),
                "fold_pass_ratio": round(fold_pass_ratio * 100, 1),
                "n_checkable_folds": len(checkable_folds),
                "n_passing_folds": len(passing_folds),
                "deployed": deployed,
                "trained_rows": len(df_train)
            }
        })

        if not deployed:
            reasons = []
            if total_signals < MIN_SIGNAL_COUNT:
                reasons.append(f"মোট সিগন্যাল অপ্রতুল ({total_signals} < {MIN_SIGNAL_COUNT})")
            if overall_precision < MIN_SIGNAL_PRECISION:
                reasons.append(f"সার্বিক প্রিসিশন অপ্রতুল ({overall_precision:.2%} < {MIN_SIGNAL_PRECISION:.0%})")
            if len(checkable_folds) < 3:
                reasons.append("যথেষ্ট পরিসংখ্যানগত-বৈধ ফোল্ড নেই")
            elif fold_pass_ratio < MIN_FOLD_PASS_RATIO:
                reasons.append(f"এজ ধারাবাহিক না ({fold_pass_ratio:.0%} ফোল্ড পাস < {MIN_FOLD_PASS_RATIO:.0%})")
            print(f"[AI Training] ডিপ্লয় হয়নি — {'; '.join(reasons)}। পুরনো মডেলই সচল থাকছে।", flush=True)
            return

        # যথেষ্ট ও ধারাবাহিক এজ প্রমাণিত হলে পুরো ডাটা দিয়ে চূড়ান্ত মডেল ফিট করে RAM-এ রাখা
        final_model = RandomForestRegressor(n_estimators=150, max_depth=8, random_state=42, n_jobs=-1)
        final_model.fit(X, y)

        bundle = {
            "model": final_model,
            "threshold_long": float(y.quantile(1 - SIGNAL_PERCENTILE)),
            "threshold_short": float(y.quantile(SIGNAL_PERCENTILE)),
            "n_features": len(features.FEATURE_COLS)
        }

        ai_model = bundle  # RAM-এ রাখা, সেভ করা হবে না

        # BANDWIDTH OPTIMIZATION: মডেল সেভ বন্ধ — পরবর্তীতে চালু করতে হলে কমেন্ট সরিয়ে ফেলুন
        # save_model_to_db(bundle)
        # try:
        #     with open(MODEL_FILE, 'wb') as f:
        #         pickle.dump(bundle, f)
        # except Exception as disk_err:
        #     print(f"Warning: could not save model file locally: {disk_err}", flush=True)

        print(f"[AI Training] নতুন এআই মডেল Walk-Forward ভ্যালিডেশন পাস করে RAM-এ সক্রিয় করা হয়েছে! (সার্বিক প্রিসিশন: {overall_precision:.2%}, ফোল্ড কনসিস্টেন্সি: {fold_pass_ratio:.0%})", flush=True)
    except Exception as e:
        print(f"[AI Training] Critical error in background training: {e}", flush=True)


def _is_valid_bundle(obj, n_features_expected):
    """পুরনো ফরম্যাটের (raw classifier) বা ভুল ফিচার-সংখ্যার মডেল বাতিল করার জন্য"""
    if not isinstance(obj, dict):
        return False
    if "model" not in obj or "threshold_long" not in obj or "threshold_short" not in obj:
        return False
    model = obj["model"]
    if hasattr(model, 'n_features_in_') and model.n_features_in_ != n_features_expected:
        return False
    return True


async def load_or_train_ai_model(global_btc_price_ref=None):
    """BANDWIDTH OPTIMIZED: Fresh train only, no loading from DB/file. RAM only."""
    global ai_model
    n_features_expected = len(features.FEATURE_COLS)

    # BANDWIDTH OPT: DB load commented out — always fresh train
    # if DB_ENABLED:
    #     try:
    #         temp_bundle = load_model_from_db()
    #         if temp_bundle is not None:
    #             if _is_valid_bundle(temp_bundle, n_features_expected):
    #                 ai_model = temp_bundle
    #                 print("[AI] Regression model bundle successfully loaded from cloud database into memory!", flush=True)
    #                 return
    #             else:
    #                 print(f"[Database] পুরনো মডেল ফরম্যাট/ফিচার অসামঞ্জস্যপূর্ণ (নতুন রিগ্রেশন বান্ডল প্রয়োজন)। রি-ট্রেন করা হচ্ছে...", flush=True)
    #                 ai_model = None
    #     except Exception as e:
    #         print(f"Cloud model load warning: {e}", flush=True)

    # BANDWIDTH OPT: Local file load commented out — always fresh train
    # if os.path.exists(MODEL_FILE) and ai_model is None:
    #     try:
    #         local_bundle = None
    #         with open(MODEL_FILE, 'rb') as f:
    #             local_bundle = pickle.load(f)
    #         if local_bundle is not None:
    #             if _is_valid_bundle(local_bundle, n_features_expected):
    #                 ai_model = local_bundle
    #                 print("[AI] Regression model bundle successfully loaded from local disk!", flush=True)
    #                 return
    #             else:
    #                 print("[Local] পুরনো লোকাল মডেল ফরম্যাট/ফিচার অসামঞ্জস্যপূর্ণ। রি-ট্রেন করা হচ্ছে...", flush=True)
    #                 ai_model = None
    #     except Exception as e:
    #         print(f"Error loading AI Model from disk: {e}", flush=True)
    #         ai_model = None

    # Always fresh train — RAM only
    if ai_model is None:
        print(f"[AI] RAM-এ উপযুক্ত {n_features_expected}-ফিচারের রিগ্রেশন মডেল নেই — fresh train শুরু হচ্ছে...", flush=True)
        print("[AI] Automatically training + validating (walk-forward) first regression model from historical data...", flush=True)

        df = state_manager.global_df.copy()
        if df.empty or len(df) < 800:
            print("[AI] Not enough candle data, skipping training.", flush=True)
            return

        state_manager.update({"wait_reason": "এআই প্রথমবার Walk-Forward ভ্যালিডেশনসহ ট্রেইন হচ্ছে... 🤖"})
        await live_bus.broadcast(state_manager.get())

        await asyncio.to_thread(train_model_in_background, df, global_btc_price_ref)

        if ai_model is None:
            print("[AI] First training run did not pass walk-forward validation - bot will run in rule-based mode (no AI) until a future retrain proves a consistent edge.", flush=True)
