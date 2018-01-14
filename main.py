from time import strftime
import time
import logging
from flask import Flask, g
from forum import *
app = Flask(__name__)
app.register_blueprint(forum_blueprint, url_prefix='/api')

# logger = logging.getLogger('__name__')

# @app.before_request
# def before_request():
#     g.start_time = time.time()
#     g.request_time = lambda: "%.5fs" % (time.time() - g.start_time)

# @app.after_request
# def after_request(response):
#     logger.error('%s %s %s %s', str(g.request_time()), request.method, request.full_path, response.status)
#     return response

@app.errorhandler(404)
def page_not_found(error):
    return Response(
        status=404,
        response=json.dumps({'message': str(error)}),
        mimetype="application/json"
    )

@app.errorhandler(409)
def page_conflict(error):
    return Response(
        status=409,
        response=json.dumps({'message': str(error)}),
        mimetype="application/json"
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0')
