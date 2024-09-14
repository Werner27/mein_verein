
import os
import pandas as pd
from pandas.errors import EmptyDataError


class ClDataframeHelper:
    """
    Eine Klasse, die beim Lesen von Daten aus einer CSV-Datei hilft.

    Attribute:
    file_path (str): Der Dateipfad der CSV-Datei.
    """

    def __init__(self, file_path):
        """
        Der Konstruktor für die CsvReader-Klasse.

        Args:
        file_path (str): Der Dateipfad der CSV-Datei.
        """
        self.file_path = file_path

    def read_csv(self, csv_name: str, filter_conditions: dict = None, return_format: str = None):
        """
        Liest eine CSV-Datei und führt verschiedene Operationen auf ihr aus.

        Args:
            csv_name (str): Der Name der CSV-Datei, die gelesen werden soll.
            filter_conditions (dict, optional): Ein Wörterbuch, das Filterbedingungen enthält.
                Schlüssel (str): Der Spaltenname im DataFrame.
                Wert: Der Wert, nach dem gefiltert werden soll.
            return_format (str, optional): Das gewünschte Rückgabeformat ("DataFrame", "dict" oder "json").

        Returns:
            pd.DataFrame or dict or str: Je nach `return_format` kann entweder der DataFrame, ein Dictionary oder ein JSON-String zurückgegeben werden.

        Raises:
            FileNotFoundError: Wenn die angegebene CSV-Datei nicht gefunden wird.
            ValueError: Wenn `return_format` einen ungültigen Wert enthält.
        """
        try:
            path = os.path.join(self.file_path, csv_name)
            if not os.path.exists(path):
                raise FileNotFoundError(f'read_csv#1 Die Datei {path} existiert nicht.')

            # Lese die CSV-Datei und speichere sie in einem Pandas DataFrame
            try:
                df = pd.read_csv(path, on_bad_lines='warn')
                # Berechnen der Anzahl der Jahre bis zur aktuellen Jahreszeit
                # df["Jahre"] = (pd.to_datetime("today").year - pd.to_datetime(df["Eintrittsdatum"]).dt.year)
                # df["Jahre"] = (pd.to_datetime("today").dt.year - pd.to_datetime(df["Datum"]).dt.year)
                """
                try:
                    df[df.columns[3]] = pd.to_datetime(df[df.columns[3]])
                    df[df.columns[4]] = pd.to_datetime(df[df.columns[4]])
                except ValueError as e:
                    raise ValueError(f'read_csv#2 Fehler beim Konvertieren des Datums in Datei {path}: {e}')
            """
            except EmptyDataError:
                raise ValueError(f'read_csv#3 Die Datei {path} enthält keine Daten.')

            if filter_conditions is not None:
                df = ClDataframeHelper.filter_dataframe(df, filter_conditions)

            if len(df.index) <= 0:
                raise ValueError(f'read_csv#4 Die Datei(df) {path} enthält keine Daten.')

            if return_format == "dict":
                return df.to_dict('records')
            elif return_format == "json":
                return df.to_json()
            elif return_format == "DataFrame" or return_format is None:
                return df
            else:
                raise ValueError(f"read_csv#5 Ungültiger Wert für return_format: {return_format}")

        except FileNotFoundError as e:
            raise FileNotFoundError("Die Datei wurde nicht gefunden.") from e

    @staticmethod
    def filter_dataframe(df: pd.DataFrame, filter_conditions: dict) -> pd.DataFrame:
        """
        Filtert einen DataFrame basierend auf den angegebenen Filterbedingungen.

        Args:
            df (pd.DataFrame): Der Eingabe-Datenrahmen, der gefiltert werden soll.
            filter_conditions (dict): Ein Wörterbuch, das die Filterbedingungen enthält.
                Schlüssel (str): Der Spaltenname im DataFrame.
                Wert: Der Wert, nach dem gefiltert werden soll.

        Returns:
            pd.DataFrame: Ein neuer DataFrame, der nur die Zeilen enthält, die den Filterbedingungen entsprechen,
                und der die Spalten enthält, die nicht in den Filterbedingungen enthalten sind.

        Raises:
            ValueError: Wenn eine der Filterbedingungen einen unbekannten Spaltennamen verwendet.

        """
        # Überprüfen, ob die Filterbedingungen gültig sind
        valid_columns = set(df.columns)
        for col_name in filter_conditions.keys():
            if col_name not in valid_columns:
                raise ValueError(f"filter_dataframe#1 Unbekannter Spaltenname: {col_name}")

        # Initialisieren einer leeren Maske
        mask = pd.Series(True, index=df.index)

        # Fülle die Maske aus und erstelle eine Liste der zu entfernenden Spalten
        col_drop = []
        for col_name, key in filter_conditions.items():
            mask = mask & (df[col_name] == key)
            col_drop.append(col_name)

        # Die Daten mit der erstellten Maske filtern und Spalten entfernen
        # filtered_df = df[mask].drop(col_drop, axis=1)

        # return filtered_df
        return df[mask]

    def update_csv(self, csv_name: str, updated_member) -> int:

        path_csv = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path_csv):
            raise FileNotFoundError(f'read_csv Die Datei {path_csv} existiert nicht.')

        path_temp = os.path.join(self.file_path, csv_name + '.tmp')

        try:
            # Lesen der CSV-Datei in einen DataFrame
            df = pd.read_csv(path_csv)

            if updated_member['ID'] == 0:
                # Ermitteln der ersten unbenutzten ID
                new_id = self.get_first_unused_id(df)
                if new_id is None:
                    raise ValueError("Keine unbenutzte ID verfügbar")
                updated_member['ID'] = new_id

                df = self.insert_member(df, updated_member)
            else:
                # Suchen und Aktualisieren der Zeile mit der angegebenen ID
                df.loc[df['ID'] == updated_member['ID'], ['Vorname', 'Nachname', 'Geburtsdatum', 'Eintrittsdatum', 'Status']] = \
                    updated_member['Vorname'], updated_member['Nachname'], updated_member['Geburtsdatum'], updated_member[
                        'Eintrittsdatum'], updated_member['Status']

            # Schreiben der aktualisierten Daten in eine temporäre Datei
            df.to_csv(path_temp, index=False)

            # Ersetzen der Originaldatei durch die temporäre Datei
            os.replace(path_temp, path_csv)
            return updated_member['ID']

        except Exception as e:
            # Löschen der temporären Datei im Fehlerfall
            if os.path.exists(path_temp):
                os.remove(path_temp)
            raise e

    def get_first_unused_id(self, df, max_id: int=1000) -> int:
        used_ids = set(df['ID'])
        for id in range(1, max_id + 1):
            if id not in used_ids:
                return id
        return None  # Falls alle IDs bis max_id verwendet sind

    def update_id_csv(self, csv_name: str, id: int, updated_member):

        path_csv = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path_csv):
            raise FileNotFoundError(f'read_csv Die Datei {path_csv} existiert nicht.')

        path_temp = os.path.join(self.file_path, csv_name + '.tmp')

        try:
            # Lesen der CSV-Datei in einen DataFrame
            df = pd.read_csv(path_csv, index_col=None)
            # aktuelle id der neuen Daten entfernen
            df_cleaned = df.drop(df[df['ID'] == id].index)

            # Umwandeln der neuen Daten in einen DataFrame
            df_insert = pd.DataFrame(updated_member)

            # Hinzufügen der neuen Zeile
            df_new = pd.concat([df_cleaned, df_insert], ignore_index=True)

            # Schreiben der aktualisierten Daten in eine temporäre Datei
            df_new.to_csv(path_temp, index=False)

            # Ersetzen der Originaldatei durch die temporäre Datei
            os.replace(path_temp, path_csv)

        except Exception as e:
            # Löschen der temporären Datei im Fehlerfall
            if os.path.exists(path_temp):
                os.remove(path_temp)
            raise e

    def insert_member(self, df, row) -> int:
        # Ermitteln der ersten unbenutzten ID
        new_id = self.get_first_unused_id(df)
        if new_id is None:
            raise ValueError("Keine unbenutzte ID verfügbar")

        row['ID'] = new_id

        # Umwandeln des neuen Mitglieds in einen DataFrame
        row_df = pd.DataFrame([row])

        # Hinzufügen der neuen Zeile
        df = pd.concat([df, row_df], ignore_index=True)

        return df

    def insert_csv(self, csv_name: str, new_member) -> int:

        path_csv = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path_csv):
            raise FileNotFoundError(f'read_csv Die Datei {path_csv} existiert nicht.')

        path_temp = os.path.join(self.file_path, csv_name + '.tmp')

        try:
            # Lesen der CSV-Datei in einen DataFrame
            df = pd.read_csv(path_csv)

            # Ermitteln der ersten unbenutzten ID
            new_id = self.get_first_unused_id(df)
            if new_id is None:
                raise ValueError("Keine unbenutzte ID verfügbar")

            new_member['ID'] = new_id

            # Umwandeln des neuen Mitglieds in einen DataFrame
            new_member_df = pd.DataFrame([new_member])

            # Debug-Ausgabe: Inhalt von new_member_df
            print("new_member_df:")
            print(new_member_df)

            # Hinzufügen der neuen Zeile
            df = pd.concat([df, new_member_df], ignore_index=True)

            # Debug-Ausgabe: Inhalt von df nach dem Anhängen
            print("df nach dem Hinzufügen:")
            print(df.tail())  # Zeige die letzten Zeilen, um das neue Mitglied zu sehen

            # Schreiben der aktualisierten Daten in eine temporäre Datei
            df.to_csv(path_temp, index=False)

            # Ersetzen der Originaldatei durch die temporäre Datei
            os.replace(path_temp, path_csv)
            return new_id

        except Exception as e:
            # Löschen der temporären Datei im Fehlerfall
            if os.path.exists(path_temp):
                os.remove(path_temp)
            raise e

    def delete_id_csv(self, csv_name: str, id: int):

        path_csv = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path_csv):
            raise FileNotFoundError(f'read_csv Die Datei {path_csv} existiert nicht.')

        path_temp = os.path.join(self.file_path, csv_name + '.tmp')

        try:
            # Lesen der CSV-Datei in einen DataFrame
            df = pd.read_csv(path_csv, index_col=None)
            # Zeilen mir der id aus Datei entfernen
            df_cleaned = df.drop(df[df['ID'] == id].index)

            # Schreiben der aktualisierten Daten in eine temporäre Datei
            df_cleaned.to_csv(path_temp, index=False)

            # Ersetzen der Originaldatei durch die temporäre Datei
            os.replace(path_temp, path_csv)

        except Exception as e:
            # Löschen der temporären Datei im Fehlerfall
            if os.path.exists(path_temp):
                os.remove(path_temp)
            raise e

    def delete_id_row_csv(self, csv_name: str, id: int, row_nr: int):

        path_csv = os.path.join(self.file_path, csv_name)
        if not os.path.exists(path_csv):
            raise FileNotFoundError(f'read_csv Die Datei {path_csv} existiert nicht.')

        path_temp = os.path.join(self.file_path, csv_name + '.tmp')

        try:
            # Lesen der CSV-Datei in einen DataFrame
            df = pd.read_csv(path_csv, index_col=None)
            # Zeilen mir der id aus Datei entfernen
            index_to_delete = df[df['ID'] == id].index[row_nr - 1]
            df_cleaned = df.drop(index_to_delete)

            # Schreiben der aktualisierten Daten in eine temporäre Datei
            df_cleaned.to_csv(path_temp, index=False)

            # Ersetzen der Originaldatei durch die temporäre Datei
            os.replace(path_temp, path_csv)

        except Exception as e:
            # Löschen der temporären Datei im Fehlerfall
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
