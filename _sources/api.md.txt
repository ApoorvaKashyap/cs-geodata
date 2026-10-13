# FastAPI Endpoints

The `cs-geodata` FastAPI service exposes several endpoints for vector data processing.

## Base Endpoints

### `GET /`
Health check to verify Redis connection and worker status.
* **Returns**: JSON object indicating connection status and number of active workers.

### `GET /api/v1/status`
Check the status of a specific background job.
* **Query Parameters**:
  * `task_id` (str): The ID of the task to check.
* **Returns**: Job status.

## Vector Endpoints

### `POST /api/v1/vector/layers`
Submit a request to convert vector layers.
* **Request Body**: JSON mapping to `ConversionRequest` schema.

### `POST /api/v1/vector/create_base_cache`
Submit a request to create a base cache for layers.
* **Request Body**: JSON mapping to `BaseLayers` schema.
