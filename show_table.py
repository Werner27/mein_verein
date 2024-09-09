from flask import render_template, Request
from dataframe_helper import ClDataframeHelper
import pandas as pd
from datetime import datetime


class ClShowTable(ClDataframeHelper):

    def __init__(self):
        """
        Initialize the request with the given Flask request and info string.
        """
        # super().__init__('/home/Werner2711/mysite/daten')
        super().__init__('daten')  # Measurement cycle time for Read external temperature
        self._filter = None
        self._df = None
        self._id = 0
        self._param = None

    def read_request(self, request: Request):
        self._id = int(request.args.get('id', 0))
        self._filter = request.form.get('filter', 'k')
        self._param = {"id": self._id, "filter": self._filter}

    def get_id(self):
        return self._id

    def render_temp_table(self, csv_name, csv_2_name=None):

        page = 'show_table.html'
        try:
            if self._param['filter'] == '25':
                filter_conditions = {'Jahre': 25}
            elif self._param['filter'] == '40':
                filter_conditions = {'Jahre': 40}
            else:
                filter_conditions = None

            df_mitglieder = self.read_csv(csv_name)
            if csv_2_name is not None:
                df_mitglieder.drop(['Geburtsdatum', 'Eintrittsdatum', 'Status'], axis=1, inplace=True)
                df_mitglieder = self.merge_file_2(df_mitglieder, csv_2_name, filter_conditions)
            else:
                df_mitglieder["Jahre"] = (pd.to_datetime("today").year -
                                          pd.to_datetime(df_mitglieder["Eintrittsdatum"]).dt.year)
                if filter_conditions is not None:
                    df_mitglieder = ClDataframeHelper.filter_dataframe(df_mitglieder, filter_conditions)

            table = df_mitglieder.to_dict('records')
            error_str = None
        except Exception as e:
            error_str = e
            table = False

        return render_template(page, call_page='mitglieder', table=table,
                                   error_str=error_str, param=self._param)

    def merge_file_2(self, df, csv_name, filter_conditions):

        df_2 = self.read_csv(csv_name)
        # Aktuelles Datum holen
        dt_str = datetime.now().strftime("%d.%m.%Y")
        # Ersetzen von NaN-Werten in der 'Bis'-Spalte
        df_2['Bis'].fillna(dt_str, inplace=True)
        df_2["Jahre"] = (pd.to_datetime(df_2["Bis"], format='%d.%m.%Y').dt.year -
                                pd.to_datetime(df_2["Von"], format='%d.%m.%Y').dt.year)
        df_2['Gesamtjahre'] = df_2.groupby('ID')['Jahre'].transform('sum')
        if filter_conditions is not None:
            df_2 = ClDataframeHelper.filter_dataframe(df_2, filter_conditions)

        # Beide df zusammenführen
        merged_df = df_2.merge(df, left_on='ID', right_on='ID', how='left')
        # Die Spaltenreihenfolge ändern
        return merged_df[['ID', 'Vorname', 'Nachname', 'Position', 'Von', 'Bis', 'Jahre', 'Gesamtjahre']]

    def request_details(self, request: Request):
        print(request.url)
        action = request.args.get('action', '')
        if action == "new":
            self._id = 0
        elif action == "update":
            mitglied = {'ID': int(request.form.get('ID', 0)), 'Vorname': request.form.get('Vorname', 'No Name'),
                        'Nachname': request.form.get('Nachname', 'No Name'),
                        'Geburtsdatum': request.form.get('Geburtsdatum', datetime(2000, 1, 1).strftime("%d.%m.%Y")),
                        'Eintrittsdatum': request.form.get('Eintrittsdatum', datetime.now().strftime("%d.%m.%Y")),
                        'Status': request.form.get('Status', '2')}
            self._id = self.update_csv('mitglieder.csv', mitglied)
        else:
            self._id = int(request.args.get('id', 0))

        self._param = {"id": self._id,}

    def render_temp_details(self, csv_name, csv_2_name=None):
        table_vorstand = False
        try:
            if self._id == 0:
                table = [{'ID': 0,
                          'Vorname': 'Vorname',
                          'Nachname': 'Nachname',
                          'Geburtsdatum': datetime(2000, 1, 1).strftime("%d.%m.%Y"),
                          'Eintrittsdatum': datetime.now().strftime("%d.%m.%Y"),
                          'Status': '2'
                          }]
                title = "Neues Mitglied"
            else:
                table = self.read_csv(csv_name, return_format='dict', filter_conditions={"ID": self._id})
                title = "Mitglied: " + table[0]['Vorname'] + ' ' + table[0]['Nachname'] + ' ' + 'ID=' + str(self._id)
                if csv_2_name is not None:
                    try:
                        df = self.read_csv(csv_2_name, filter_conditions={"ID": self._id})
                        df.drop(['ID'], axis=1, inplace=True)
                        table_vorstand = df.to_dict('records')
                        # table_vorstand = self.read_csv(csv_2_name, return_format='dict', filter_conditions={"ID": self._id})
                    except:
                        pass

        except Exception as e:
            title = e
            table = False

        templ = render_template('mitglied.html', title=title, table=table, table_vorstand=table_vorstand)
        return templ


if __name__ == "__main__":
    o_show_table = ClShowTable()
    html = o_show_table.render_temp_table('mitglieder.csv')
    print(html)
