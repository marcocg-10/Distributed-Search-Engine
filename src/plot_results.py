import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

RESULTS_DIR = Path("results")
OUTPUT_DIR = RESULTS_DIR / "plots"
OUTPUT_DIR.mkdir(exist_ok=True)

# Prueba 1: documentos vs tiempo
test1 = pd.read_csv(RESULTS_DIR / "test_1.csv")
summary1 = (
    test1
    .groupby("value")
    .agg(
        mean_time=("time_seconds", "mean"),
        std_time=("time_seconds", "std"),
        mean_size=("size_kb", "mean")
    )
    .reset_index()
)
plt.figure(figsize=(8, 5))
plt.errorbar(
    summary1["value"],
    summary1["mean_time"],
    yerr=summary1["std_time"],
    marker="o",
    capsize=5
)
plt.xlabel("Cantidad de documentos")
plt.ylabel("Tiempo promedio de indexación (segundos)")
plt.title("Impacto de la cantidad de documentos en el tiempo de indexación")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "01_tiempo_indexacion_documentos.png", dpi=300)
plt.show()

# Prueba 2: particiones vs tiempo
test2 = pd.read_csv(RESULTS_DIR / "test_2.csv")
summary2 = (
    test2
    .groupby("value")
    .agg(
        mean_time=("time_seconds", "mean"),
        std_time=("time_seconds", "std")
    )
    .reset_index()
)
plt.figure(figsize=(8, 5))
plt.errorbar(
    summary2["value"],
    summary2["mean_time"],
    yerr=summary2["std_time"],
    marker="o",
    capsize=5
)
plt.xlabel("Número de particiones")
plt.ylabel("Tiempo promedio de indexación (segundos)")
plt.title("Impacto de número de particiones en el tiempo de indexación")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "02_tiempo_indexacion_particiones.png", dpi=300)
plt.show()


# Prueba 3: speedup por particiones
base_time = summary2.loc[summary2["value"] == summary2["value"].min(), "mean_time"].iloc[0]
summary2["speedup"] = base_time / summary2["mean_time"]
plt.figure(figsize=(8, 5))
plt.plot(summary2["value"], summary2["speedup"], marker="o")
plt.xlabel("Número de particiones")
plt.ylabel("Speedup")
plt.title("Speedup relativo según particiones")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "03_speedup_particiones.png", dpi=300)
plt.show()


# Prueba 4: tiempo de búsqueda
test3 = pd.read_csv(RESULTS_DIR / "test_3.csv")
summary3 = (
    test3
    .groupby("value")
    .agg(
        mean_time=("time_seconds", "mean"),
        std_time=("time_seconds", "std")
    )
    .reset_index()
)
plt.figure(figsize=(10, 5))
plt.bar(summary3["value"], summary3["mean_time"], yerr=summary3["std_time"], capsize=5)
plt.xlabel("Consulta")
plt.ylabel("Tiempo promedio de búsqueda (segundos)")
plt.title("Tiempo de búsqueda por consulta")
plt.xticks(rotation=25, ha="right")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "04_tiempo_busqueda_consulta.png", dpi=300)
plt.show()
