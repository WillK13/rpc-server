import glob, csv, re
import statistics as stats
import matplotlib.pyplot as plt

def load_csv(path):
    lats=[]
    with open(path) as f:
        rdr=csv.DictReader(f)
        for row in rdr:
            lats.append(float(row["latency_ms"]))
    lats.sort()
    n=len(lats)
    def pct(p):
        i=round((p/100)*(n-1))
        return lats[i]
    return {
        "count": n,
        "avg": sum(lats)/n,
        "p50": pct(50),
        "p95": pct(95),
        "p99": pct(99),
    }

# 1) Load-Latency (mix)
mix_points=[]
for fn in sorted(glob.glob("results/mix_rps_*.csv"),
                 key=lambda s: int(re.search(r"mix_rps_(\d+)\.csv", s).group(1))):
    rps=int(re.search(r"mix_rps_(\d+)\.csv", fn).group(1))
    m=load_csv(fn)
    mix_points.append((rps,m))

xs=[r for r,_ in mix_points]
p50=[m["p50"] for _,m in mix_points]
p95=[m["p95"] for _,m in mix_points]
avg=[m["avg"] for _,m in mix_points]

plt.figure()
plt.plot(xs, avg, label="avg")
plt.plot(xs, p50, label="p50")
plt.plot(xs, p95, label="p95")
plt.xlabel("Offered load (RPS)")
plt.ylabel("Latency (ms)")
plt.title("Load–Latency (mixed workload)")
plt.legend(); plt.grid(True)
plt.savefig("results/load_latency_mix.png", dpi=160)

# 2) Throughput knee heuristic (first point where p95 > 2x min p50)
min_p50=min(p50) if p50 else 0.0
knee=None
for r, m in mix_points:
    if m["p95"] > 2.0*min_p50:
        knee=(r, m["p95"]); break
if knee:
    print(f"Knee (heuristic): ~{knee[0]} RPS where p95 ~ {knee[1]:.2f}ms")

# 3) Operation comparison (p95 vs RPS)
def load_series(prefix):
    pts=[]
    for fn in sorted(glob.glob(f"results/{prefix}_rps_*.csv"),
                     key=lambda s: int(re.search(rf"{prefix}_rps_(\d+)\.csv", s).group(1))):
        rps=int(re.search(rf"{prefix}_rps_(\d+)\.csv", fn).group(1))
        m=load_csv(fn)
        pts.append((rps,m))
    return pts

ops=["hash","sort","matmul","compress"]
plt.figure()
for op in ops:
    pts=load_series(op)
    if not pts: continue
    xs=[r for r,_ in pts]
    p95=[m["p95"] for _,m in pts]
    plt.plot(xs, p95, label=op)
plt.xlabel("Offered load (RPS)")
plt.ylabel("p95 latency (ms)")
plt.title("Operation Comparison (p95 vs RPS)")
plt.legend(); plt.grid(True)
plt.savefig("results/op_comparison_p95.png", dpi=160)

print("Wrote results/load_latency_mix.png and results/op_comparison_p95.png")
