
import requests
import shutil
from pathlib import Path

API = "https://datasets-server.huggingface.co/parquet"
DATASET = "adde023/steam-reviews"

folder = Path("data/bronze")
folder.mkdir(parents=True, exist_ok=True)

session = requests.Session()

# Récupérer tous les fichiers Parquet
response = session.get(
    API,
    params={"dataset": DATASET},
    timeout=60
)
response.raise_for_status()

files = response.json()["parquet_files"]

if not files:
    raise RuntimeError("Aucun fichier disponible")

total_gb = sum(f["size"] for f in files) / (1024**3)

print(f"Fichiers disponibles : {len(files)}")
print(f"Taille totale : {total_gb:.2f} Go")

# Télécharger tous les fichiers
for i, file in enumerate(files, start=1):

    destination = folder / f"steam_reviews_{i:03d}.parquet"
    temp = folder / f"steam_reviews_{i:03d}.part"
    expected_size = file["size"]

    # Ignorer les fichiers déjà complets
    if destination.exists():
        if destination.stat().st_size == expected_size:
            print(f"[{i}/{len(files)}] Déjà téléchargé")
            continue

    # Vérifier l'espace disponible
    free_space = shutil.disk_usage(folder).free

    if free_space < expected_size + 20 * 1024**3:
        raise RuntimeError("Espace disque insuffisant")

    print(f"[{i}/{len(files)}] Téléchargement...", flush=True)

    try:
        with session.get(
            file["url"],
            stream=True,
            timeout=(30, 120)
        ) as r:
            r.raise_for_status()

            with open(temp, "wb") as output:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        output.write(chunk)

        if temp.stat().st_size != expected_size:
            raise RuntimeError("Fichier incomplet")

        temp.replace(destination)
        print("Terminé !", flush=True)

    except Exception:
        temp.unlink(missing_ok=True)
        raise

print("\nTOUS LES FICHIERS TÉLÉCHARGÉS !")
