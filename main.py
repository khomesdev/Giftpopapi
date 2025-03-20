from flask import Flask, request, jsonify
import threading
from utils import process_voucher_async

app = Flask(__name__)


@app.route("/getvoucher", methods=["POST"])
def haravan_webhook():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No JSON payload received"}), 400

    # Lấy email từ payload
    customer_email = data.get("email")
    if not customer_email:
        return jsonify({"status": "error", "message": "Missing customer email"}), 400

    # Lấy order_number
    raw_order_number = data.get("order_number")
    if not raw_order_number:
        return jsonify({"status": "error", "message": "Missing order_number"}), 400

    if raw_order_number.startswith("#"):
        base_order_no = "KHOMES" + raw_order_number[1:]
    else:
        base_order_no = "KHOMES" + raw_order_number

    # Lấy danh sách line_items
    line_items = data.get("line_items", [])
    if not line_items:
        return jsonify({"status": "error", "message": "Missing line_items"}), 400

    # Phản hồi webhook ngay lập tức
    response = jsonify({
        "status": "success",
        "message": "Webhook received. Voucher processing will be done asynchronously.",
        "customer_email": customer_email,
        "order_no": base_order_no
    })

    # Xử lý voucher và gửi email ở background
    threading.Thread(target=process_voucher_async,
                     args=(data, base_order_no)).start()

    return response, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
