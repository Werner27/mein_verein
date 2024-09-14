import pandas as pd

# Beispiel-Daten
data = [
    {'ID': 1, 'Name': 'Alice', 'Age': 25},
    {'ID': 2, 'Name': 'Bob', 'Age': 30},
    {'ID': 2, 'Name': 'Bob1', 'Age': 31},
    {'ID': 2, 'Name': 'Bob2', 'Age': 32},
    {'ID': 3, 'Name': 'Charlie', 'Age': 35}
]

# DataFrame erstellen
df = pd.DataFrame(data)

# ID für die Löschung
target_id = 2
instance_to_delete = 2  # Wir wollen die 2. Zeile mit ID=2 löschen (0-basiert)

# Zeilen mit ID=2 filtern
filtered_rows = df[df['ID'] == target_id]

# Den Index der 2. Zeile mit ID=2 ermitteln (1-basiert, deshalb [instance_to_delete - 1])
index_to_delete = filtered_rows.index[instance_to_delete - 1]

# Die Zeile mit diesem Index löschen
df = df.drop(index_to_delete)

# Resultierender DataFrame
print(df)

# End Programm
