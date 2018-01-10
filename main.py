from flask import Flask
from forum import *
app = Flask(__name__)
app.register_blueprint(forum_blueprint)

# @app.route('/')
# def hello_world():
#     return 'Hello, World!'

@app.errorhandler(404)
def page_not_found(error):
    return Response(
        status=404,
        response=json.dumps({'message': str(error)}),
        mimetype="application/json"
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0')
