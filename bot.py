# train_model.py (ম্যানুয়াল/অফলাইন ট্রেনিং ও ভ্যালিডেশন স্ক্রিপ্ট - Walk-Forward সংস্করণ)
# BANDWIDTH OPTIMIZATION:
#   - save_model_to_db() COMMENTED OUT (no cloud save)
#   - pickle.dump() COMMENTED OUT (no local file save)
#   - Training logic UNCHANGED (walk-forward, same validation)
#
# ai_engine.py-এর মতোই এখন Walk-Forward ভ্যালিডেশন ব্যবহার করে, যাতে ম্যানুয়াল রান আর
# লাইভ বট সবসময় একই (এবং সবচেয়ে কঠোর) মানদণ্ডে যাচাই করে।
import os
import pickle
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from config import DB_ENABLED, HISTORY_FILE
# BANDWIDTH OPT: Commented out DB model save
# from database import engine, save_model_to_db
from database import engine
import features
from ai_engine import _walk_forward_validate, N_FOLDS, MIN_SIGNAL_COUNT, MIN_SIGNAL_PRECISION, MIN_FOLD_PASS_RATIO, MIN_SIGNALS_PER_FOLD_FOR_CHECK, SIGNAL_PERCENTILE

MODEL_FILE = "trading_model.pkl"


def train_ai_model():
    df = None

    if DB_ENABLED and engine is not None:
        print("Reading historical data from cloud database (Supabase)...", flush=True)
        try:
            df = pd.read_sql("SELECT * FROM sol_15m_history ORDER BY t ASC", engine)
            print(f"Loaded {len(df)} candles from cloud database.", flush=True)
        except Exception as e:
            print(f"Warning reading cloud database: {e}", flush=True)
            df = None

    if df is None or df.empty:
        if os.path.exists(HISTORY_FILE):
            print(f"Loading data from local file '{HISTORY_FILE}' instead...", flush=True)
            df = pd.read_csv(HISTORY_FILE)
            df = df.sort_values('t').reset_index(drop=True)
        else:
            print("Error: no data found in cloud database or local history file!", flush=True)
            return

    if len(df) < 800:
        print(f"Warning: not enough data for walk-forward validation (have {len(df)} candles, need at least 800).", flush=True)
        return

    print("Building features (shared pipeline, aligned BTC history from DB)...", flush=True)
    df = features.build_features(df)
    df['target'] = features.make_regression_target(df)
    df = df.dropna(subset=['target']).reset_index(drop=True)

    X = df[features.FEATURE_COLS].fillna(0)
    y = df['target']

    print(f"Running {N_FOLDS}-fold walk-forward validation on {len(X)} rows...", flush=True)
    fold_results = _walk_forward_validate(X, y, N_FOLDS)

    if not fold_results:
        print("Not enough data to form walk-forward folds.", flush=True)
        return

    for r in fold_results:
        prec_str = f"{r['precision']:.2%}" if r['precision'] is not None else "N/A"
        print(f"  ফোল্ড {r['fold']}: ট্রেইন {r['train_rows']} | টেস্ট {r['test_rows']} | সিগন্যাল {r['n_signals']} (L:{r['n_long']}/S:{r['n_short']}) | প্রিসিশন {prec_str}")

    total_signals = sum(r["n_signals"] for r in fold_results)
    total_correct = sum(r["n_correct"] for r in fold_results)
    overall_precision = (total_correct / total_signals) if total_signals > 0 else 0.0

    checkable_folds = [r for r in fold_results if r["n_signals"] >= MIN_SIGNALS_PER_FOLD_FOR_CHECK]
    passing_folds = [r for r in checkable_folds if r["precision"] is not None and r["precision"] >= 0.50]
    fold_pass_ratio = (len(passing_folds) / len(checkable_folds)) if checkable_folds else 0.0

    print(f"\nমোট সিগন্যাল: {total_signals} | সার্বিক প্রিসিশন: {overall_precision:.2%} | ফোল্ড কনসিস্টেন্সি: {len(passing_folds)}/{len(checkable_folds)} ({fold_pass_ratio:.0%})")

    deployable = (
        total_signals >= MIN_SIGNAL_COUNT and
        overall_precision >= MIN_SIGNAL_PRECISION and
        len(checkable_folds) >= 3 and
        fold_pass_ratio >= MIN_FOLD_PASS_RATIO
    )

    if not deployable:
        print("\n⚠️  এই মডেল walk-forward ভ্যালিডেশন পাস করেনি — লাইভ বটের গেটও এটা রিজেক্ট করবে।")
        print("এটা এখনো সেভ হবে (পরিদর্শনের জন্য), কিন্তু ডিপ্লয় করার আগে ফিচার/ডাটা আরও উন্নত করা উচিত।")
    else:
        print("\n✅ এই মডেল walk-forward ভ্যালিডেশন পাস করেছে — লাইভ বটের গেটও এটা ডিপ্লয় করবে।")

    # পুরো ডাটা দিয়ে চূড়ান্ত মডেল ফিট করে থ্রেশহোল্ডসহ বান্ডল (RAM only, no save)
    final_model = RandomForestRegressor(n_estimators=150, max_depth=8, random_state=42, n_jobs=-1)
    final_model.fit(X, y)

    bundle = {
        "model": final_model,
        "threshold_long": float(y.quantile(1 - SIGNAL_PERCENTILE)),
        "threshold_short": float(y.quantile(SIGNAL_PERCENTILE)),
        "n_features": len(features.FEATURE_COLS)
    }

    # BANDWIDTH OPTIMIZATION: মডেল সেভ বন্ধ — পরবর্তীতে চালু করতে হলে কমেন্ট সরিয়ে ফেলুন
    # save_model_to_db(bundle)
    # with open(MODEL_FILE, 'wb') as f:
    #     pickle.dump(bundle, f)
    # print(f"\nModel bundle saved locally and to cloud database (if enabled).")
    
    print(f"\nModel bundle trained successfully (RAM only, not saved).")
    print(f"To deploy: ai_model = bundle (in RAM)")


if __name__ == "__main__":
    train_ai_model()
