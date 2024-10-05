import os
import pandas as pd
from pandas.errors import EmptyDataError


class ClDataframeHelper:
    """
    Eine Klasse, die beim Lesen, Bearbeiten und Löschen von Daten aus einer CSV-Datei hilft.

    Attribute:
        file_path (str): Der Dateipfad des Ordners, in dem sich die CSV-Dateien befinden.
    """

    def __init__(self, file_path: str):
        if not file_path:
            raise ValueError("file_path darf nicht leer sein.")
        self.file_path = file_path

    def read_csv(self, csv_name: str, filter_conditions: dict = None, return_format: str = 'DataFrame'):
        """
        Liest eine CSV-Datei und wendet optional Filterbedingungen an.

        Args:
            csv_name (str): Der Name der zu lesenden CSV-Datei.
            filter_conditions (dict, optional): Ein Wörterbuch mit Filterbedingungen.
                Schlüssel (str): Der Spaltenname im DataFrame.
                Wert: Der Wert, nach dem gefiltert werden soll.
            return_format (str, optional): Das Format, in dem die Daten zurückgegeben werden sollen.
                Mögliche Werte: 'DataFrame', 'dict', 'json'. Standard ist 'DataFrame'.

        Returns:
            pd.DataFrame | dict | str: Der gefilterte DataFrame oder die Daten im gewünschten Format.

        Raises:
            FileNotFoundError: Wenn die CSV-Datei nicht gefunden wird.
            ValueError: Wenn ein ungültiges `return_format` angegeben wird.
            pd.errors.EmptyDataError: Wenn die CSV-Datei leer ist.
        """
        path = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Die Datei {path} existiert nicht.")

        try:
            df = pd.read_csv(path)
        except EmptyDataError:
            raise EmptyDataError("Die CSV-Datei ist leer.")

        if filter_conditions is not None:
            df = ClDataframeHelper.filter_dataframe(df, filter_conditions)

        if return_format == 'DataFrame':
            return df
        elif return_format == 'dict':
            return df.to_dict(orient='records')
        elif return_format == 'json':
            return df.to_json(orient='records')
        else:
            raise ValueError(f"Ungültiges Rückgabeformat: {return_format}. Erlaubt sind 'DataFrame', 'dict', 'json'.")

    @staticmethod
    def filter_dataframe(df: pd.DataFrame, filter_conditions: dict) -> pd.DataFrame:
        """
        Filtert einen DataFrame basierend auf den angegebenen Bedingungen.

        Args:
            df (pd.DataFrame): Der zu filternde DataFrame.
            filter_conditions (dict): Ein Wörterbuch mit Filterbedingungen.

        Returns:
            pd.DataFrame: Der gefilterte DataFrame.

        Raises:
            ValueError: Wenn eine Spalte in den Filterbedingungen nicht im DataFrame existiert.
        """
        if filter_conditions is not None:
            for column, value in filter_conditions.items():
                if column not in df.columns:
                    raise ValueError(f"Spalte {column} existiert nicht im DataFrame.")
                df = df[df[column] == value]
        return df

    def update_csv(self, csv_name: str, id: int, updated_data: dict):
        """
        Aktualisiert eine Zeile in der CSV-Datei basierend auf der ID.

        Args:
            csv_name (str): Der Name der CSV-Datei.
            id (int): Die ID der Zeile, die aktualisiert werden soll.
            updated_data (dict): Die neuen Daten als Dictionary.

        Raises:
            FileNotFoundError: Wenn die CSV-Datei nicht existiert.
            ValueError: Wenn die ID nicht in der CSV-Datei gefunden wird oder die Spaltennamen in updated_data nicht stimmen.
        """
        path_csv = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path_csv):
            raise FileNotFoundError(f"Die Datei {path_csv} existiert nicht.")

        path_temp = os.path.join(self.file_path, csv_name + '.tmp')

        try:
            df = pd.read_csv(path_csv)
            if 'ID' not in df.columns:
                raise ValueError("Die Spalte 'ID' existiert nicht in der CSV-Datei.")
            if id not in df['ID'].values:
                raise ValueError(f"Keine Zeilen mit der ID {id} gefunden.")

            index_to_update = df[df['ID'] == id].index[0]
            for column, value in updated_data.items():
                if column not in df.columns:
                    raise ValueError(f"Spalte {column} existiert nicht in der CSV-Datei.")
                df.at[index_to_update, column] = value

            df.to_csv(path_temp, index=False)
            os.replace(path_temp, path_csv)

        except Exception as e:
            if os.path.exists(path_temp):
                os.remove(path_temp)
            raise e

    def get_first_unused_id(self, csv_name: str):
        """
        Gibt die erste unbenutzte ID in der CSV-Datei zurück.

        Args:
            csv_name (str): Der Name der CSV-Datei.

        Returns:
            int: Die erste unbenutzte ID.

        Raises:
            FileNotFoundError: Wenn die CSV-Datei nicht existiert.
        """
        path = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Die Datei {path} existiert nicht.")

        try:
            df = pd.read_csv(path)
            if df.empty or 'ID' not in df.columns:
                return 1
            used_ids = set(df['ID'])
            unused_id = 1
            while unused_id in used_ids:
                unused_id += 1
            return unused_id

        except EmptyDataError:
            return 1

    def insert_csv(self, csv_name: str, rows_data: list):
        """
        Fügt mehrere neue Zeilen in die CSV-Datei ein und generiert für jede Zeile, deren ID auf 0 gesetzt ist, eine eindeutige ID.

        Args:
            csv_name (str): Der Name der CSV-Datei, in die die Daten eingefügt werden sollen.
            rows_data (list): Eine Liste von Dictionaries, die die einzufügenden Zeilen repräsentieren.
                              Jede Zeile muss den Schlüssel 'ID' enthalten, der initial auf 0 gesetzt sein kann.

        Raises:
            FileNotFoundError: Wenn die CSV-Datei nicht existiert.
            ValueError: Wenn die Spaltennamen in `rows_data` nicht mit den Spalten der CSV-Datei übereinstimmen oder die Daten inkonsistent sind.

        Returns:
            None: Die Methode gibt nichts zurück, schreibt aber die neuen Daten in die CSV-Datei.

        Beschreibung:
            Diese Methode fügt mehrere Zeilen in die angegebene CSV-Datei ein. Jede Zeile muss ein Dictionary sein, das die
            Spaltennamen als Schlüssel und die entsprechenden Werte enthält. Wenn der Wert der ID in einer Zeile auf 0 gesetzt ist,
            wird eine neue, einzigartige ID generiert, die noch nicht in der CSV-Datei verwendet wurde.
        """
        path_csv = os.path.join(self.file_path, csv_name)

        # Überprüfen, ob die Datei existiert
        if not os.path.exists(path_csv):
            raise FileNotFoundError(f"Die Datei {path_csv} existiert nicht.")

        path_temp = os.path.join(self.file_path, csv_name + '.tmp')

        try:
            # Lesen der vorhandenen CSV-Daten
            df = pd.read_csv(path_csv)
            if not df.empty and 'ID' not in df.columns:
                raise ValueError("Die CSV-Datei muss eine 'ID'-Spalte enthalten.")

            # Überprüfen, ob die Spalten in row_data mit den Spalten der CSV-Datei übereinstimmen
            if rows_data and set(rows_data[0].keys()) != set(df.columns):
                raise ValueError(
                    "Die Schlüsselnamen von rows_data müssen mit den Spalten der CSV-Datei übereinstimmen.")

            # Set zur Nachverfolgung der bereits verwendeten IDs
            used_ids = set(df['ID']) if not df.empty else set()

            # Generieren und Hinzufügen der neuen Zeilen
            for row_data in rows_data:
                if row_data['ID'] == 0:
                    # Generiere eine neue ID
                    new_id = 1
                    while new_id in used_ids:
                        new_id += 1
                    row_data['ID'] = new_id
                    used_ids.add(new_id)

                # Füge die neue Zeile dem DataFrame hinzu
                df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)

            # Speichern der aktualisierten Daten in einer temporären Datei
            df.to_csv(path_temp, index=False)

            # Ersetzen der Originaldatei durch die temporäre Datei
            os.replace(path_temp, path_csv)

        except Exception as e:
            # Löschen der temporären Datei im Fehlerfall
            if os.path.exists(path_temp):
                os.remove(path_temp)
            raise e

    def delete_id_csv(self, csv_name: str, id: int):
        """
        Löscht alle Zeilen mit der angegebenen ID aus der CSV-Datei.

        Args:
            csv_name (str): Der Name der CSV-Datei.
            id (int): Die ID der Zeile, die gelöscht werden soll.

        Raises:
            FileNotFoundError: Wenn die CSV-Datei nicht existiert.
            ValueError: Wenn die Spalte ID nicht vorhanden ist.
        """
        path_csv = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path_csv):
            raise FileNotFoundError(f"Die Datei {path_csv} existiert nicht.")

        path_temp = os.path.join(self.file_path, csv_name + '.tmp')

        try:
            df = pd.read_csv(path_csv)
            if 'ID' not in df.columns:
                raise ValueError(f"Die Spalte 'ID' existiert nicht in der CSV-Datei {csv_name}.")
            if id not in df['ID'].values:
                # raise ValueError(f"Keine Zeilen mit der ID {id} gefunden in der CSV-Datei {csv_name}.")
                return

            df_cleaned = df[df['ID'] != id]

            df_cleaned.to_csv(path_temp, index=False)
            os.replace(path_temp, path_csv)
        except Exception as e:
            if os.path.exists(path_temp):
                os.remove(path_temp)
            raise e

    def delete_id_row_csv(self, csv_name: str, id: int, row_nr: int):
        """
        Löscht eine Zeile mit der angegebenen ID und Reihenummer aus der CSV-Datei.

        Args:
            csv_name (str): Der Name der CSV-Datei.
            id (int): Die ID der Zeile, die gelöscht werden soll.
            row_nr (int): Die Nummer der zu löschenden Zeile mit der angegebenen ID (beginnend bei 1).

        Raises:
            FileNotFoundError: Wenn die CSV-Datei nicht existiert.
            ValueError: Wenn die ID nicht in der CSV-Datei gefunden wird oder row_nr ungültig ist.
        """
        path_csv = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path_csv):
            raise FileNotFoundError(f"Die Datei {path_csv} existiert nicht.")

        path_temp = os.path.join(self.file_path, csv_name + '.tmp')

        try:
            df = pd.read_csv(path_csv)
            if 'ID' not in df.columns:
                raise ValueError("Die Spalte 'ID' existiert nicht in der CSV-Datei.")

            matching_rows = df[df['ID'] == id]
            if matching_rows.empty:
                raise ValueError(f"Keine Zeilen mit ID {id} gefunden.")
            if row_nr < 1 or row_nr > len(matching_rows):
                raise ValueError(f"Ungültige Reihenummer {row_nr} für ID {id}.")

            index_to_delete = matching_rows.index[row_nr - 1]
            df_cleaned = df.drop(index_to_delete)

            df_cleaned.to_csv(path_temp, index=False)
            os.replace(path_temp, path_csv)
        except Exception as e:
            if os.path.exists(path_temp):
                os.remove(path_temp)
            raise e


if __name__ == "__main__":
    o_dataframe_helper = ClDataframeHelper('daten')
    # df = o_dataframe_helper.read_csv('mitglieder.csv')
    mitglied = {'ID': '0',
                'Vorname': 'Vorname',
                'Nachname': 'Nachname',
                'Geburtsdatum': '01.01.2000',
                'Eintrittsdatum': '01.08.2024',
                'Status': '2'
                }

    df = o_dataframe_helper.insert_csv('mitglieder.csv', mitglied)

    print("Kompleter DF ")
    print(df)

