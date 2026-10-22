import pandas as pd
import matplotlib.pyplot as plt


def main() -> None:
    df = pd.read_csv("results.csv")
    df_sorted = df.sort_values("time", ascending=False).copy()

    df_sorted["time-parsed"] = pd.to_timedelta(df["time"], unit="ms")
    df_sorted["time-seconds"] = df_sorted["time-parsed"].dt.total_seconds()

    fig, ax = plt.subplots(figsize=(12, 5), layout="constrained")

    bars = ax.barh(
        df_sorted["label"],
        df_sorted["time-seconds"],
        color="skyblue",
        edgecolor="black",
    )

    ax.bar_label(
        bars,
        labels=[f"{seconds:.3f}s" for seconds in df_sorted["time-seconds"]],
        padding=4,
    )

    ax.set_title("Connection Performance")
    ax.set_xlabel("Elapsed Time (seconds)")
    ax.set_ylabel("Connection")
    ax.set_xlim(left=0, right=df_sorted["time-seconds"].max() * 1.3)

    ax.set_axisbelow(True)
    ax.xaxis.grid(True, linestyle="--", alpha=0.4)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.savefig("results.png")
    plt.show()


if __name__ == "__main__":
    main()
