import azure.functions as func
import json

app = func.FunctionApp()

@app.route(route="send_order", methods=["POST"])
@app.queue_output(arg_name="outputQueue", queue_name="order-queue", connection="AzureWebJobsStorage")
def send_order(req: func.HttpRequest, outputQueue: func.Out[str]) -> func.HttpResponse:
    try:
        order_data = req.get_json()
        message = json.dumps(order_data)
        outputQueue.set(message)
        return func.HttpResponse(f"Sent message to queue: {message}", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)