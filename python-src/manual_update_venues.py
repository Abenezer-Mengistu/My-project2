import pandas as pd
from pathlib import Path
df = pd.DataFrame([
    {"name": "Ariana Grande Oakland", "stubhub_url": "https://www.stubhub.com/parking-passes-only-ariana-grande-oakland-tickets-6-6-2026/event/159278587/", "handler": "stubhub-discovery", "location": "Oakland, CA"}
])
df.to_excel("venues.xlsx", index=False)
print("Manual update of venues.xlsx complete.")
