# A very simple Flask Hello World app for you to get started with...
from flask import Flask, render_template, request
from show_table import ClShowTable

app = Flask(__name__)


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/mitglieder', methods=['GET', 'POST'])
def mitglieder():
    o_show_table = ClShowTable()
    o_show_table.read_request(request)
    return o_show_table.render_temp_table('mitglieder.csv')

@app.route('/vorstand', methods=['GET', 'POST'])
def vorstand():
    o_show_table = ClShowTable()
    o_show_table.read_request(request)
    return o_show_table.render_temp_table('mitglieder.csv', 'vorstand.csv')

@app.route('/details', methods=['GET', 'POST'])
def details():
    o_show_table = ClShowTable()
    o_show_table.request_details(request)
    return o_show_table.render_temp_details('mitglieder.csv', 'vorstand.csv')

@app.route('/test', methods=['GET', 'POST'])
def test():
    param = {"id": 0}
    title = request.args.get('title')
    if title == None:
        title = 'Mitglied'
    # print(title)
    str = request.form.get('txt', 'No-Text')
    print(str)

    return render_template('Test.html', my_text=str, param=param, title=title)


@app.route('/shutdown', methods=['GET', 'POST'])
def shutdown():
    return 'Shutdown'

if __name__ == "__main__":
    app.run(debug=False)
    print("End Main")
