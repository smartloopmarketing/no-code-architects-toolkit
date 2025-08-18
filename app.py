# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.



from flask import Flask, request, jsonify, Response
from queue import Queue
from services.webhook import send_webhook
import threading
import uuid
import os
import time
from version import BUILD_NUMBER  # Import the BUILD_NUMBER
from app_utils import log_job_status, discover_and_register_blueprints  # Import the discover_and_register_blueprints function

MAX_QUEUE_LENGTH = int(os.environ.get('MAX_QUEUE_LENGTH', 0))

def create_app():
    app = Flask(__name__)

    # Create a queue to hold tasks
    task_queue = Queue()
    queue_id = id(task_queue)  # Generate a single queue_id for this worker

    # Function to process tasks from the queue
    def process_queue():
        while True:
            job_id, data, task_func, queue_start_time = task_queue.get()
            queue_time = time.time() - queue_start_time
            run_start_time = time.time()
            pid = os.getpid()  # Get the PID of the actual processing thread
            
            # Log job status as running
            log_job_status(job_id, {
                "job_status": "running",
                "job_id": job_id,
                "queue_id": queue_id,
                "process_id": pid,
                "response": None
            })
            
            response = task_func()
            run_time = time.time() - run_start_time
            total_time = time.time() - queue_start_time

            response_data = {
                "endpoint": response[1],
                "code": response[2],
                "id": data.get("id"),
                "job_id": job_id,
                "response": response[0] if response[2] == 200 else None,
                "message": "success" if response[2] == 200 else response[0],
                "pid": pid,
                "queue_id": queue_id,
                "run_time": round(run_time, 3),
                "queue_time": round(queue_time, 3),
                "total_time": round(total_time, 3),
                "queue_length": task_queue.qsize(),
                "build_number": BUILD_NUMBER  # Add build number to response
            }
            
            # Log job status as done
            log_job_status(job_id, {
                "job_status": "done",
                "job_id": job_id,
                "queue_id": queue_id,
                "process_id": pid,
                "response": response_data
            })

            # Only send webhook if webhook_url has an actual value (not an empty string)
            if data.get("webhook_url") and data.get("webhook_url") != "":
                send_webhook(data.get("webhook_url"), response_data)

            task_queue.task_done()

    # Start the queue processing in a separate thread
    threading.Thread(target=process_queue, daemon=True).start()

    # Decorator to add tasks to the queue or bypass it
    def queue_task(bypass_queue=False):
        def decorator(f):
            def wrapper(*args, **kwargs):
                job_id = str(uuid.uuid4())
                data = request.json if request.is_json else {}
                pid = os.getpid()  # Get PID for non-queued tasks
                start_time = time.time()
                
                if bypass_queue or 'webhook_url' not in data:
                    
                    # Log job status as running immediately (bypassing queue)
                    log_job_status(job_id, {
                        "job_status": "running",
                        "job_id": job_id,
                        "queue_id": queue_id,
                        "process_id": pid,
                        "response": None
                    })
                    
                    response = f(job_id=job_id, data=data, *args, **kwargs)
                    run_time = time.time() - start_time
                    
                    response_obj = {
                        "code": response[2],
                        "id": data.get("id"),
                        "job_id": job_id,
                        "response": response[0] if response[2] == 200 else None,
                        "message": "success" if response[2] == 200 else response[0],
                        "run_time": round(run_time, 3),
                        "queue_time": 0,
                        "total_time": round(run_time, 3),
                        "pid": pid,
                        "queue_id": queue_id,
                        "queue_length": task_queue.qsize(),
                        "build_number": BUILD_NUMBER  # Add build number to response
                    }
                    
                    # Log job status as done
                    log_job_status(job_id, {
                        "job_status": "done",
                        "job_id": job_id,
                        "queue_id": queue_id,
                        "process_id": pid,
                        "response": response_obj
                    })
                    
                    return response_obj, response[2]
                else:
                    if MAX_QUEUE_LENGTH > 0 and task_queue.qsize() >= MAX_QUEUE_LENGTH:
                        error_response = {
                            "code": 429,
                            "id": data.get("id"),
                            "job_id": job_id,
                            "message": f"MAX_QUEUE_LENGTH ({MAX_QUEUE_LENGTH}) reached",
                            "pid": pid,
                            "queue_id": queue_id,
                            "queue_length": task_queue.qsize(),
                            "build_number": BUILD_NUMBER  # Add build number to response
                        }
                        
                        # Log the queue overflow error
                        log_job_status(job_id, {
                            "job_status": "done",
                            "job_id": job_id,
                            "queue_id": queue_id,
                            "process_id": pid,
                            "response": error_response
                        })
                        
                        return error_response, 429
                    
                    # Log job status as queued
                    log_job_status(job_id, {
                        "job_status": "queued",
                        "job_id": job_id,
                        "queue_id": queue_id,
                        "process_id": pid,
                        "response": None
                    })
                    
                    task_queue.put((job_id, data, lambda: f(job_id=job_id, data=data, *args, **kwargs), start_time))
                    
                    return {
                        "code": 202,
                        "id": data.get("id"),
                        "job_id": job_id,
                        "message": "processing",
                        "pid": pid,
                        "queue_id": queue_id,
                        "max_queue_length": MAX_QUEUE_LENGTH if MAX_QUEUE_LENGTH > 0 else "unlimited",
                        "queue_length": task_queue.qsize(),
                        "build_number": BUILD_NUMBER  # Add build number to response
                    }, 202
            return wrapper
        return decorator

    app.queue_task = queue_task

    # Register special route for Next.js root asset paths first
    from routes.v1.media.feedback import create_root_next_routes
    create_root_next_routes(app)
    
    # Use the discover_and_register_blueprints function to register all blueprints
    discover_and_register_blueprints(app)

    return app

app = create_app()

# --- Minimal interactive API docs (Swagger UI and ReDoc) ---
def _generate_openapi_spec(flask_app: Flask) -> dict:
    """Generate a minimal OpenAPI 3.0 spec by introspecting Flask routes.

    Note: This provides paths and methods with generic responses. It does not
    attempt to infer request/response schemas. Security header is documented.
    """
    paths: dict = {}
    for rule in flask_app.url_map.iter_rules():
        # Skip Flask's static rules or internal endpoints
        if rule.endpoint == 'static' or rule.rule.startswith('/static'):
            continue

        # Only document standard HTTP methods
        methods = [m for m in sorted(rule.methods) if m in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']]
        if not methods:
            continue

        # Convert Flask-style <param> to OpenAPI-style {param}
        path_template = rule.rule.replace('<', '{').replace('>', '}')

        path_item: dict = {}
        for method in methods:
            path_item[method.lower()] = {
                'summary': rule.endpoint,
                'responses': {
                    '200': {
                        'description': 'OK'
                    }
                },
                'security': [{'ApiKeyAuth': []}]
            }

        paths[path_template] = path_item

    spec = {
        'openapi': '3.0.3',
        'info': {
            'title': 'No-Code Architects Toolkit API',
            'version': str(BUILD_NUMBER),
            'description': 'Auto-generated minimal OpenAPI spec from Flask routes.'
        },
        'servers': [
            {'url': '/'}
        ],
        'paths': paths,
        'components': {
            'securitySchemes': {
                'ApiKeyAuth': {
                    'type': 'apiKey',
                    'in': 'header',
                    'name': 'x-api-key'
                }
            }
        },
        'security': [{'ApiKeyAuth': []}]
    }
    return spec


@app.route('/openapi.json', methods=['GET'])
def openapi_json() -> Response:
    return jsonify(_generate_openapi_spec(app))


@app.route('/docs', methods=['GET'])
def swagger_ui() -> Response:
    # Serve Swagger UI from CDN, pointing at our /openapi.json
    html = """
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title>API Docs</title>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css"/>
      </head>
      <body>
        <div id="swagger-ui"></div>
        <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
        <script>
          window.ui = SwaggerUIBundle({
            url: '/openapi.json',
            dom_id: '#swagger-ui',
            presets: [SwaggerUIBundle.presets.apis],
            layout: 'BaseLayout'
          });
        </script>
      </body>
    </html>
    """
    return Response(html, mimetype='text/html')


@app.route('/redoc', methods=['GET'])
def redoc_ui() -> Response:
    html = """
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title>API Docs</title>
        <style> body { margin: 0; padding: 0; } </style>
        <script src="https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"></script>
      </head>
      <body>
        <redoc spec-url="/openapi.json"></redoc>
      </body>
    </html>
    """
    return Response(html, mimetype='text/html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)