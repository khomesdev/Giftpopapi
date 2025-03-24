import os
import pandas as pd
import requests
import base64
import json
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import gspread
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from dotenv import load_dotenv
load_dotenv()
# ------------------------ Cấu hình kết nối Google Sheets -------------------------------
# Thay đường dẫn file JSON của bạn
SERVICE_ACCOUNT_FILE = os.environ.get('GCP_JSON_PATH')
# Thay bằng ID của file Google Sheets của bạn
SPREADSHEET_ID = "105eppmLyEWyDyzehfkd-TUrky67B093mZPZjGlNij2I"
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
spreadsheet = gc.open_by_key(SPREADSHEET_ID)

# ------------------------------ List URL -------------------------------------------------
goods_list_url = "https://sandbox-pos.giftpop.vn:9901/interface/order/goodsListAll.m12"
voucher_url = "https://sandbox-pos.giftpop.vn:9901/interface/order/voucherIssueList.m12"
order_info_url = "https://sandbox-pos.giftpop.vn:9901/interface/order/orderInfo.m12"
voucher_info_url = "https://sandbox-pos.giftpop.vn:9901/interface/order/voucherInfo.m12"
# ------------------------------ Cấu hình chung -------------------------------------------
AUTH_KEY = "S0hPTUVTOlExRjJWbkZzYW5Sd1VYUlRRVXBPU3c="
PRIVATE_KEY_PATH = os.environ.get('PRIVATE_KEY_PATH')
AES_KEY = "QPLXSH5QSEO7T450"

# Đọc private key từ file (sử dụng một lần cho cả quá trình)
with open(PRIVATE_KEY_PATH, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None
    )


def sign_payload(payload):
    """
    Chuyển payload (dict) thành JSON và ký bằng private key.
    Trả về tuple (json_data, signature_base64)
    """
    json_data = json.dumps(payload)
    signature = private_key.sign(
        json_data.encode('utf-8'),
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    signature_base64 = base64.b64encode(signature).decode('utf-8')
    return json_data, signature_base64

# Hàm giải mã pinNo sử dụng AES-128-ECB với PKCS7 padding


def decrypt_pinno(pinno, key):
    try:
        # Base64 decode
        ciphertext = base64.b64decode(pinno)
        # Create AES cipher in ECB mode
        cipher = Cipher(algorithms.AES(key.encode('utf-8')), modes.ECB())
        decryptor = cipher.decryptor()
        decrypted_bytes = decryptor.update(ciphertext) + decryptor.finalize()

        # PKCS7 unpadding
        pad_len = decrypted_bytes[-1]
        decrypted_bytes = decrypted_bytes[:-pad_len]

        return decrypted_bytes.decode('utf-8')
    except Exception as e:
        return f"Decrypt error: {e}"


def update_or_append_sheet(worksheet, new_df, key_column=None):
    """
    Nếu key_column được cung cấp và có trong header, dựa theo key đó sẽ cập nhật dòng đã có.
    Nếu không có key_column hoặc không tìm thấy key, sẽ append dòng mới vào cuối.
    Các giá trị dạng dict hoặc list sẽ được chuyển thành chuỗi JSON.
    """
    existing_data = worksheet.get_all_values()
    # Nếu sheet rỗng, thêm header
    if not existing_data:
        worksheet.append_row(list(new_df.columns))
        existing_data = worksheet.get_all_values()

    header = existing_data[0]

    def convert_value(val):
        if isinstance(val, (dict, list)):
            return json.dumps(val, ensure_ascii=False)
        return val

    if key_column and key_column in header:
        key_idx = header.index(key_column)
        # Tạo mapping: key value -> số thứ tự dòng (bắt đầu từ 2)
        key_to_row = {}
        for i, row in enumerate(existing_data[1:], start=2):
            if len(row) > key_idx:
                key_to_row[row[key_idx]] = i
        for idx, new_row in new_df.iterrows():
            key_val = str(new_row.get(key_column))
            new_row_list = [convert_value(new_row.get(col, ""))
                            for col in header]
            if key_val in key_to_row:
                row_number = key_to_row[key_val]
                # Giả sử số cột không vượt quá 26 (A-Z)
                end_col = col_num_to_letters(len(header))
                cell_range = f"A{row_number}:{end_col}{row_number}"
                worksheet.update(cell_range, [new_row_list])
            else:
                worksheet.append_row(new_row_list)
    else:
        # Nếu không có key_column, append từng dòng
        for idx, new_row in new_df.iterrows():
            new_row_list = [convert_value(new_row.get(col, ""))
                            for col in header]
            worksheet.append_row(new_row_list)
    print(f"Sheet '{worksheet.title}' đã được cập nhật.")


def get_goods_list():
    """
    Gọi API goodsListAll để lấy danh sách sản phẩm.
    Sau khi lấy thành công, cập nhật dữ liệu lên Google Sheets vào sheet 'GoodsList'.
    Trả về danh sách sản phẩm (goods_list) nếu thành công, ngược lại trả về None.
    """
    payload_goods = {"authKey": AUTH_KEY}
    json_data_goods, signature_goods = sign_payload(payload_goods)
    headers_goods = {"Signature": signature_goods}
    print("Gọi API goodsListAll để lấy danh sách sản phẩm...")
    response_goods = requests.post(
        goods_list_url, json=payload_goods, headers=headers_goods)

    if response_goods.status_code == 200:
        data_goods = response_goods.json()
        goods_list = data_goods.get('goodsList', [])
        print(f"Đã lấy được {len(goods_list)} sản phẩm.")

        # Nếu có dữ liệu, chuyển đổi thành DataFrame và cập nhật lên Google Sheets
        if goods_list:
            df_goods = pd.DataFrame(goods_list)
            try:
                worksheet = spreadsheet.worksheet("GoodsList")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title="GoodsList", rows="1000", cols="26")
                worksheet.append_row(list(df_goods.columns))
            # Giả sử key duy nhất của sản phẩm là "goodsId"
            update_or_append_sheet(worksheet, df_goods, key_column="goodsId")
        return goods_list
    else:
        print("Lỗi khi lấy danh sách sản phẩm:",
              response_goods.status_code, response_goods.text)
        return None


def process_voucher(order_no, quantity, goodsId):
    """
    Xử lý voucher cho đơn hàng:
      1. Gọi API voucherIssueList dựa trên order_no, quantity, goodsId.
      2. Tách dữ liệu voucherIssueList thành các DataFrame:  df_order_info, df_voucher_list.
      3. Gọi API orderInfo cho order_no để lấy chi tiết đơn hàng, từ đó tách ra:
             - df_order_info_detail (toàn bộ chi tiết)
             - df_order_info_detail_order (chỉ thông tin orderInfo)
             - df_order_info_detail_voucher (chỉ voucher trong orderInfo)
      4. Gọi API voucherInfo cho từng voucher (trong voucher_items) để cập nhật trạng thái,
         tạo DataFrame df_voucher_list.
      5. Cập nhật các DataFrame vào Google Sheets với các sheet tương ứng.
    Trả về một từ điển chứa các DataFrame đã xử lý.
    """
    import pandas as pd
    import requests

    # --- Bước 1: Gọi API voucherIssueList ---
    payload_voucher = {
        "authKey": AUTH_KEY,
        "goodsId": goodsId,
        "sendType": "API",
        "smsYN": "N",
        "rcvPhoneNo": "",
        "sendTitle": "",
        "sendMsg": "",
        "sendLang": "",
        "quantity": quantity,
        "orderNo": order_no,
        "note": ""
    }
    json_data_voucher, signature_voucher = sign_payload(payload_voucher)
    headers_voucher = {"Signature": signature_voucher}
    print(
        f"\nGọi API voucherIssueList cho goodsId {goodsId} với orderNo {order_no}...")
    response_voucher = requests.post(
        voucher_url, json=payload_voucher, headers=headers_voucher)
    if response_voucher.status_code != 200:
        print(
            f"Lỗi cho goodsId {goodsId}: {response_voucher.status_code} {response_voucher.text}")
        return None

    voucher_response = response_voucher.json()
    voucher_response['goodsId'] = goodsId

    # --- Bước 2: Tách dữ liệu voucherIssueList ---
    # 2.2 Tách cột orderInfo
    order_info_list = []
    order_info = voucher_response.get('orderInfo', {})
    row = dict(order_info)
    row['goodsId'] = voucher_response.get('goodsId')
    order_info_list.append(row)
    df_order_info = pd.DataFrame(order_info_list)

    # 2.3 Tách cột voucherList
    voucher_items = []
    order_no_extracted = voucher_response.get('orderInfo', {}).get('orderNo')
    for v in (voucher_response.get('voucherList') or []):
        row = dict(v)
        row['goodsId'] = goodsId
        row['orderNo'] = order_no_extracted
        if 'pinNo' in row and row['pinNo']:
            row['decryptedPin'] = decrypt_pinno(row['pinNo'], AES_KEY)
        else:
            row['decryptedPin'] = None
        voucher_items.append(row)
    df_voucher_list = pd.DataFrame(voucher_items)

    # --- Bước 5: Cập nhật dữ liệu lên Google Sheets ---
    sheets_data = {
        "OrderInfo": (df_order_info, "orderNo"),
        "VoucherList": (df_voucher_list, "orderNo")
        # "OrderInfoDetailOrder": (df_order_info_detail_order, "orderNo"),
        # "OrderInfoDetailVoucher": (df_order_info_detail_voucher, "orderNo"),
        # "VoucherInfoDetail": (df_updated_voucher_info_detail, "orderNo")
    }
    for sheet_name, (df_sheet, key_col) in sheets_data.items():
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name, rows="1000", cols="26")
            worksheet.append_row(list(df_sheet.columns))
        update_or_append_sheet(worksheet, df_sheet, key_column=key_col)

    # Trả về một từ điển chứa tất cả các DataFrame cần thiết
    return {
        "df_order_info": df_order_info,
        "df_voucher_list": df_voucher_list,
        # "df_order_info_detail": df_order_info_detail,
        # "df_order_info_detail_order": df_order_info_detail_order,
        # "df_order_info_detail_voucher": df_order_info_detail_voucher,
        # "df_updated_voucher_info_detail": df_updated_voucher_info_detail
    }


def send_voucher_email(customer_email, extended_order_no, goodsId, quantity, voucher_list, title, product_image):
    """
    Gửi email thông báo voucher cho khách hàng sử dụng mẫu HTML mới.

    LƯU Ý:
      - Google đã tắt "Less secure apps". Bạn phải tạo App Password
        (mật khẩu ứng dụng) mới gửi mail được nếu dùng Gmail.
      - Tham khảo https://support.google.com/accounts/answer/185833 để bật xác thực 2 bước và tạo App Password.
      - Thay thế 'sender_password' bằng App Password thật, không phải mật khẩu Gmail thông thường.
    """
    # ------------------------------------------
    # Cấu hình SMTP
    smtp_host = "smtp.gmail.com"
    smtp_port = 587
    sender_email = "admin@khomes.com.vn"
    display_name = "Ưu đãi Doanh Nghiệp Corporate Offers "
    sender_password = "chqh wcwr bjws rwgd"  # Sử dụng App Password

    # Tiêu đề email
    subject = f"Thông tin Voucher cho đơn {extended_order_no}"

    # Lấy nội dung HTML từ hàm get_email_html_body
    html_body = get_email_html_body(
        title, voucher_list, product_image)

    # Tạo message MIME dạng HTML
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = formataddr((display_name, sender_email))
    message["To"] = customer_email

    part_html = MIMEText(html_body, "html", "utf-8")
    message.attach(part_html)

    # Gửi email
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()  # Kích hoạt TLS
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, customer_email, message.as_string())
        print(
            f"Đã gửi email cho {customer_email} về đơn hàng {extended_order_no}.")
    except Exception as e:
        print(f"Lỗi khi gửi email: {e}")


def get_email_html_body(title, voucher_list, product_image):
    # Xây dựng các dòng (tr) chứa mã và nút
    dynamic_rows = ""
    for voucher in voucher_list:
        voucher_code = voucher.get("voucher_code", "")
        coupon_href = voucher.get(
            "coupon_href", "https://thammyvienngocdung.com/coupon-code/"
        )

        dynamic_rows += f"""
          <tr>
            <!-- Cột chứa mã voucher -->
            <td style="padding:5px 10px; text-align:center; vertical-align:middle;">
              <strong 
                style="font-size:18px; color:#203354; font-family:'Montserrat', 'Trebuchet MS', 'Lucida Grande', 
                       'Lucida Sans Unicode', 'Lucida Sans', Tahoma, sans-serif;">
                {voucher_code}
              </strong>
            </td>
            <!-- Cột chứa nút -->
            <td style="padding:5px 10px; text-align:center; vertical-align:middle;">
              <a 
                href="{coupon_href}" 
                target="_blank" 
                style="color:#ffffff; text-decoration:none;"
              >
                <span 
                  class="button" 
                  style="
                    background-color:#203354; 
                    border:2px solid #54239C; 
                    border-radius:2px; 
                    color:#ffffff; 
                    display:inline-block; 
                    font-family:'Montserrat', 'Trebuchet MS', 'Lucida Grande', 
                                 'Lucida Sans Unicode', 'Lucida Sans', Tahoma, sans-serif; 
                    font-size:14px; 
                    font-weight:700; 
                    padding:5px 15px; 
                    letter-spacing:1px;"
                >
                  NHẬN QUÀ NGAY
                </span>
              </a>
            </td>
          </tr>
        """

    # Bọc các dòng trên vào 1 bảng để căn thẳng hàng
    dynamic_buttons_table = f"""
      <table 
        align="center" 
        style="margin:0 auto; border-collapse:collapse;" 
        border="0" 
        cellpadding="0" 
        cellspacing="0"
      >
        <tbody>
          {dynamic_rows}
        </tbody>
      </table>
    """
    return f"""<!DOCTYPE html>
<html
  xmlns:v="urn:schemas-microsoft-com:vml"
  xmlns:o="urn:schemas-microsoft-com:office:office"
  lang="en"
>
  <head>
    <title></title>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <!--[if mso]><xml>
        <o:OfficeDocumentSettings>
          <o:PixelsPerInch>96</o:PixelsPerInch>
          <o:AllowPNG />
        </o:OfficeDocumentSettings>
      </xml><![endif]-->
    <!--[if !mso]><!-->
    <link
      href="https://fonts.googleapis.com/css?family=Roboto"
      rel="stylesheet"
      type="text/css"
    />
    <link
      href="https://fonts.googleapis.com/css2?family=Montserrat:wght@100;200;300;400;500;600;700;800;900"
      rel="stylesheet"
      type="text/css"
    />
    <!--<![endif]-->
    <style>
      * {{
        box-sizing: border-box;
      }}

      body {{
        margin: 0;
        padding: 0;
      }}

      a[x-apple-data-detectors] {{
        color: inherit !important;
        text-decoration: inherit !important;
      }}

      #MessageViewBody a {{
        color: inherit;
        text-decoration: none;
      }}

      p {{
        line-height: inherit;
      }}

      .desktop_hide,
      .desktop_hide table {{
        mso-hide: all;
        display: none;
        max-height: 0px;
        overflow: hidden;
      }}

      .image_block img + div {{
        display: none;
      }}

      sup,
      sub {{
        font-size: 75%;
        line-height: 0;
      }}

      #converted-body .list_block ul,
      #converted-body .list_block ol,
      .body [class~="x_list_block"] ul,
      .body [class~="x_list_block"] ol,
      u + .body .list_block ul,
      u + .body .list_block ol {{
        padding-left: 20px;
      }}

      @media (max-width: 620px) {{
        .desktop_hide table.icons-inner {{
          display: inline-block !important;
        }}

        .icons-inner {{
          text-align: center;
        }}

        .icons-inner td {{
          margin: 0 auto;
        }}

        .image_block div.fullWidth {{
          max-width: 100% !important;
        }}

        .mobile_hide {{
          display: none;
        }}

        .row-content {{
          width: 100% !important;
        }}

        .stack .column {{
          width: 100%;
          display: block;
        }}

        .mobile_hide {{
          min-height: 0;
          max-height: 0;
          max-width: 0;
          overflow: hidden;
          font-size: 0px;
        }}

        .desktop_hide,
        .desktop_hide table {{
          display: table !important;
          max-height: none !important;
        }}

        .row-7 .column-1 .block-1.heading_block td.pad,
        .row-7 .column-1 .block-6.list_block td.pad {{
          padding: 15px !important;
        }}

        .row-7 .column-1 .block-2.heading_block td.pad {{
          padding: 5px 10px 10px !important;
        }}

        .row-7 .column-1 .block-6.list_block ul {{
          line-height: auto !important;
        }}
      }}
    </style>
    <!--[if mso]><style>
        sup,
        sub {{
          font-size: 100% !important;
        }}
        sup {{
          mso-text-raise: 10%;
        }}
        sub {{
          mso-text-raise: -10%;
        }}
      </style><![endif]-->
  </head>

  <body
    class="body"
    style="background-color: #ffffff; margin: 0; padding: 0; -webkit-text-size-adjust: none; text-size-adjust: none;"
  >
    <table
      class="nl-container"
      width="100%"
      border="0"
      cellpadding="0"
      cellspacing="0"
      role="presentation"
      style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #ffffff;"
    >
      <tbody>
        <tr>
          <td>
            <table
              class="row row-1"
              align="center"
              width="100%"
              border="0"
              cellpadding="0"
              cellspacing="0"
              role="presentation"
              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt"
            >
              <tbody>
                <tr>
                  <td>
                    <table
                      class="row-content stack"
                      align="center"
                      border="0"
                      cellpadding="0"
                      cellspacing="0"
                      role="presentation"
                      style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; border-radius: 0; color: #000000; width: 600px; margin: 0 auto;"
                      width="600"
                    >
                      <tbody>
                        <tr>
                          <td
                            class="column column-1"
                            width="100%"
                            style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; font-weight: 400; text-align: left; padding-bottom: 5px; padding-top: 5px; vertical-align: top;"
                          >
                            <table
                              class="image_block block-1"
                              width="100%"
                              border="0"
                              cellpadding="0"
                              cellspacing="0"
                              role="presentation"
                              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                            >
                              <tr>
                                <td
                                  class="pad"
                                  style="padding-bottom: 10px; padding-left: 60px; padding-right: 60px; padding-top: 10px; width: 100%;"
                                >
                                  <div class="alignment" align="center" style="line-height: 10px">
                                    <div class="fullWidth" style="max-width: 240px">
                                      <a
                                        href="https://corporateoffers.com.vn/"
                                        target="_blank"
                                        style="outline: none"
                                        tabindex="-1"
                                      ><img
                                        src="https://bf857b141c.imgdist.com/pub/bfra/x1jkkl08/5wj/icr/om0/logo%20ngang%20t%C3%A1ch%20n%E1%BB%81n.png"
                                        style="display: block; height: auto; border: 0; width: 100%;"
                                        width="240"
                                        alt
                                        title
                                        height="auto"
                                      /></a>
                                    </div>
                                  </div>
                                </td>
                              </tr>
                            </table>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
              </tbody>
            </table>
            <table
              class="row row-2"
              align="center"
              width="100%"
              border="0"
              cellpadding="0"
              cellspacing="0"
              role="presentation"
              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #fafaff;"
            >
              <tbody>
                <tr>
                  <td>
                    <table
                      class="row-content stack"
                      align="center"
                      border="0"
                      cellpadding="0"
                      cellspacing="0"
                      role="presentation"
                      style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #ffffff; color: #000000; width: 600px; margin: 0 auto;"
                      width="600"
                    >
                      <tbody>
                        <tr>
                          <td
                            class="column column-1"
                            width="100%"
                            style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; font-weight: 400; text-align: left; vertical-align: top;"
                          >
                            <table
                            class="image_block block-1"
                            width="100%"
                            border="0"
                            cellpadding="0"
                            cellspacing="0"
                            role="presentation"
                            style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                          >
                            <tr>
                              <td class="pad" style="width: 100%">
                                <div class="alignment" align="center" style="line-height: 10px">
                                  <!-- Thay thế div cũ bằng div có width/height cố định + overflow:hidden -->
                                  <div style="width:600px; height:420px; overflow:hidden; margin: 0 auto;">
                                    <a
                                      href="https://corporateoffers.com.vn/collections/e-voucher"
                                      target="_blank"
                                      style="outline: none"
                                      tabindex="-1"
                                    >
                                      <!-- Ảnh chiếm 100% div, cao 100% div, cắt phần thừa bằng object-fit -->
                                      <img
                                        src="{product_image}"
                                        style="display: block; width: 100%; height: 100%; object-fit: cover; object-position: center; border: 0;"
                                        alt="collage of close up portraits"
                                        title="collage of close up portraits"
                                      />
                                    </a>
                                  </div>
                                </div>
                              </td>
                            </tr>
                          </table>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
              </tbody>
            </table>
            <table
              class="row row-3"
              align="center"
              width="100%"
              border="0"
              cellpadding="0"
              cellspacing="0"
              role="presentation"
              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #fafaff;"
            >
              <tbody>
                <tr>
                  <td>
                    <table
                      class="row-content stack"
                      align="center"
                      border="0"
                      cellpadding="0"
                      cellspacing="0"
                      role="presentation"
                      style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #ffffff; color: #000000; width: 600px; margin: 0 auto;"
                      width="600"
                    >
                      <tbody>
                        <tr>
                          <td
                            class="column column-1"
                            width="100%"
                            style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; font-weight: 400; text-align: left; padding-bottom: 5px; padding-left: 15px; padding-right: 15px; padding-top: 5px; vertical-align: top;"
                          >
                            <div
                              class="spacer_block block-1"
                              style="height: 30px; line-height: 30px; font-size: 1px;"
                            >
                              &#8202;
                            </div>
                            <table
                              class="heading_block block-2"
                              width="100%"
                              border="0"
                              cellpadding="0"
                              cellspacing="0"
                              role="presentation"
                              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                            >
                              <tr>
                                <td
                                  class="pad"
                                  style="padding-bottom: 10px; padding-top: 5px; text-align: center; width: 100%;"
                                >
                                  <h1
                                    style="margin: 0; color: #203354; direction: ltr; font-family: 'Montserrat', 'Trebuchet MS', 'Lucida Grande', 'Lucida Sans Unicode', 'Lucida Sans', Tahoma, sans-serif; font-size: 16px; font-weight: 400; letter-spacing: 1px; line-height: 120%; text-align: center; margin-top: 0; margin-bottom: 0; mso-line-height-alt: 19.2px;"
                                  >
                                    Cảm ơn bạn đã mua thành công
                                  </h1>
                                </td>
                              </tr>
                            </table>
                            <table
                              class="heading_block block-3"
                              width="100%"
                              border="0"
                              cellpadding="0"
                              cellspacing="0"
                              role="presentation"
                              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                            >
                              <tr>
                                <td
                                  class="pad"
                                  style="padding-bottom: 10px; padding-top: 5px; text-align: center; width: 100%;"
                                >
                                  <h1
                                    style="margin: 0; color: #203354; direction: ltr; font-family: 'Montserrat', 'Trebuchet MS', 'Lucida Grande', 'Lucida Sans Unicode', 'Lucida Sans', Tahoma, sans-serif; font-size: 30px; font-weight: 700; letter-spacing: normal; line-height: 120%; text-align: center; margin-top: 0; margin-bottom: 0; mso-line-height-alt: 36px;"
                                  >
                                    <span class="tinyMce-placeholder" style="word-break: break-word">{title}</span>
                                  </h1>
                                </td>
                              </tr>
                            </table>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
              </tbody>
            </table>
            <table
              class="row row-6"
              align="center"
              width="100%"
              border="0"
              cellpadding="0"
              cellspacing="0"
              role="presentation"
              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #fafaff;"
            >
              <tbody>
                <tr>
                  <td>
                    <table
                      class="row-content stack"
                      align="center"
                      border="0"
                      cellpadding="0"
                      cellspacing="0"
                      role="presentation"
                      style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #42a5ff; color: #000000; width: 600px; margin: 0 auto;"
                      width="600"
                    >
                      <tbody>
                        <tr>
                          <td
                            class="column column-1"
                            width="100%"
                            style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; font-weight: 400; text-align: left; padding-bottom: 10px; padding-top: 15px; vertical-align: top;"
                          >
                            <table
                              class="heading_block block-1"
                              width="100%"
                              border="0"
                              cellpadding="0"
                              cellspacing="0"
                              role="presentation"
                              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                            >
                              <tr>
                                <td
                                  class="pad"
                                  style="padding-bottom: 10px; padding-top: 5px; text-align: center; width: 100%;"
                                >
                                  <h1
                                    style="margin: 0; color: #ffffff; direction: ltr; font-family: 'Montserrat', 'Trebuchet MS', 'Lucida Grande', 'Lucida Sans Unicode', 'Lucida Sans', Tahoma, sans-serif; font-size: 20px; font-weight: 700; letter-spacing: normal; line-height: 120%; text-align: center; margin-top: 0; margin-bottom: 0; mso-line-height-alt: 24px;"
                                  >
                                    <span class="tinyMce-placeholder" style="word-break: break-word">MÃ VOUCHER</span>
                                  </h1>
                                </td>
                              </tr>
                            </table>
                                        <!-- Phần voucher với nút riêng cho từng voucher (dynamic buttons) -->
            <table class="row row-3" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #fafaff;">
              <tbody>
                <tr>
                  <td>
                    <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width: 600px; margin: 0 auto;" width="600">
                      <tbody>
                        <tr>
                          <td class="column column-1" width="100%" style="padding: 15px;">
                            {dynamic_buttons_table}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
              </tbody>
            </table>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
              </tbody>
            </table>
            <table
              class="row row-7"
              align="center"
              width="100%"
              border="0"
              cellpadding="0"
              cellspacing="0"
              role="presentation"
              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt"
            >
              <tbody>
                <tr>
                  <td>
                    <table
                      class="row-content stack"
                      align="center"
                      border="0"
                      cellpadding="0"
                      cellspacing="0"
                      role="presentation"
                      style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; border-radius: 0; color: #000000; width: 600px; margin: 0 auto;"
                      width="600"
                    >
                      <tbody>
                        <tr>
                          <td
                            class="column column-1"
                            width="100%"
                            style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; font-weight: 400; text-align: left; padding-bottom: 5px; padding-top: 5px; vertical-align: top;"
                          >
                            <table
                              class="heading_block block-1"
                              width="100%"
                              border="0"
                              cellpadding="10"
                              cellspacing="0"
                              role="presentation"
                              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                            >
                              <tr>
                                <td class="pad">
                                  <h1
                                    style="margin: 0; color: #203354; direction: ltr; font-family: 'Montserrat', 'Trebuchet MS', 'Lucida Grande', 'Lucida Sans Unicode', 'Lucida Sans', Tahoma, sans-serif; font-size: 23px; font-weight: 700; letter-spacing: normal; line-height: 120%; text-align: center; margin-top: 0; margin-bottom: 0; mso-line-height-alt: 27.599999999999998px;"
                                  >
                                    <span class="tinyMce-placeholder" style="word-break: break-word">HƯỚNG DẪN KÍCH HOẠT E-VOUCHER</span>
                                  </h1>
                                </td>
                              </tr>
                            </table>
                            <table
                              class="heading_block block-2"
                              width="100%"
                              border="0"
                              cellpadding="0"
                              cellspacing="0"
                              role="presentation"
                              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                            >
                              <tr>
                                <td
                                  class="pad"
                                  style="padding-bottom: 10px; padding-top: 5px; text-align: center; width: 100%;"
                                >
                                  <h1
                                    style="margin: 0; color: #203354; direction: ltr; font-family: 'Montserrat', 'Trebuchet MS', 'Lucida Grande', 'Lucida Sans Unicode', 'Lucida Sans', Tahoma, sans-serif; font-size: 18px; font-weight: 400; letter-spacing: normal; line-height: 150%; text-align: center; margin-top: 0; margin-bottom: 0; mso-line-height-alt: 27px;"
                                  >
                                    <strong>Kích hoạt mã code</strong>&nbsp;bằng
                                    cách nhập mã voucher hoặc click vào nút
                                    &nbsp;<strong>NHẬN QUÀ NGAY</strong>. Vui lòng xem danh sách cửa hàng áp dụng và đọc kĩ điều kiện sử dụng voucher trước khi dùng.
                                  </h1>
                                </td>
                              </tr>
                            </table>
                            <table
                              class="button_block block-3"
                              width="100%"
                              border="0"
                              cellpadding="10"
                              cellspacing="0"
                              role="presentation"
                              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                            >
                            </table>
                            <table
                              class="divider_block block-4"
                              width="100%"
                              border="0"
                              cellpadding="10"
                              cellspacing="0"
                              role="presentation"
                              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                            >
                              <tr>
                                <td class="pad">
                                  <div class="alignment" align="center">
                                    <table
                                      border="0"
                                      cellpadding="0"
                                      cellspacing="0"
                                      role="presentation"
                                      width="100%"
                                      style="mso-table-lspace: 0pt; mso-table-rspace: 0pt;"
                                    >
                                      <tr>
                                        <td
                                          class="divider_inner"
                                          style="font-size: 1px; line-height: 1px; border-top: 1px solid #dddddd;"
                                        >
                                          <span style="word-break: break-word">&#8202;</span>
                                        </td>
                                      </tr>
                                    </table>
                                  </div>
                                </td>
                              </tr>
                            </table>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
              </tbody>
            </table>
            <table
              class="row row-8"
              align="center"
              width="100%"
              border="0"
              cellpadding="0"
              cellspacing="0"
              role="presentation"
              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #fafaff; background-position: center top;"
            >
              <tbody>
                <tr>
                  <td>
                    <table
                      class="row-content stack"
                      align="center"
                      border="0"
                      cellpadding="0"
                      cellspacing="0"
                      role="presentation"
                      style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #fafaff; color: #000000; width: 600px; margin: 0 auto;"
                      width="600"
                    >
                      <tbody>
                        <tr>
                          <td
                            class="column column-1"
                            width="100%"
                            style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; font-weight: 400; text-align: left; padding-bottom: 40px; padding-left: 40px; padding-right: 40px; padding-top: 40px; vertical-align: top;"
                          >
                            <table
                              class="paragraph_block block-1"
                              width="100%"
                              border="0"
                              cellpadding="10"
                              cellspacing="0"
                              role="presentation"
                              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; word-break: break-word;"
                            >
                              <tr>
                                <td class="pad">
                                  <div
                                    style="color: #000000; font-family: Roboto, Tahoma, Verdana, Segoe, sans-serif; font-size: 14px; line-height: 120%; text-align: center; mso-line-height-alt: 16.8px;"
                                  >
                                    <p style="margin: 0; word-break: break-word">
                                      Công Ty TNHH K-Homès · 102 Street No. 2, Binh Tho Ward, Thu Duc City · Ho Chi Minh city, Viet Nam 700000 · Vietnam
                                    </p>
                                  </div>
                                </td>
                              </tr>
                            </table>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
              </tbody>
            </table>
            <table
              class="row row-9"
              align="center"
              width="100%"
              border="0"
              cellpadding="0"
              cellspacing="0"
              role="presentation"
              style="mso-table-lspace: 0pt; mso-table-rspace: 0pt; background-color: #ffffff;"
            ></table>
          </td>
        </tr>
      </tbody>
    </table>
    <!-- End -->
  </body>
</html>"""


def process_voucher_async(data, base_order_no):
    """Xử lý voucher, gửi email sau khi webhook đã được phản hồi."""
    customer_email = data.get("email")
    line_items = data.get("line_items", [])
    n_line_items = len(line_items)
    all_voucher_info = []
    # Lấy danh sách sản phẩm từ API
    goods_list = get_goods_list()
    if goods_list is None:
        print("Không lấy được danh sách sản phẩm. Bỏ qua xử lý voucher.")
        return
    # Giả sử các sản phẩm trong goods_list chứa key 'sku'
    valid_skus = {product.get("goodsId")
                  for product in goods_list if product.get("goodsId")}

    for idx, item in enumerate(line_items):
        quantity = item.get("quantity", 1)
        goodsId = item.get("sku")
        title = item.get("title", "No Title")
        image_field = item.get("image", {})
        if isinstance(image_field, dict):
            product_image = image_field.get(
                "src", "https://bf857b141c.imgdist.com/pub/bfra/x1jkkl08/xp0/k7t/6ey/NgocDung.png")
        else:
            product_image = image_field or "https://bf857b141c.imgdist.com/pub/bfra/x1jkkl08/xp0/k7t/6ey/NgocDung.png"
        if not goodsId:
            print(f"Missing goodsId (sku) in line item {idx+1}")
            continue
        if goodsId not in valid_skus:
            print(
                f"goodsId {goodsId} không tồn tại trong danh sách sản phẩm. Bỏ qua xử lý cho line item {idx+1}.")
            continue

        # Xây dựng extended_order_no
        if n_line_items == 1:
            extended_order_no = base_order_no
        else:
            extended_order_no = f"{base_order_no}_{idx+1}"

        # Gọi hàm xử lý voucher
        result = process_voucher(extended_order_no, quantity, goodsId)
        if result is None:
            print(f"Processing voucher failed for order {extended_order_no}")
            continue

        # Lấy dataframe voucher
        df_voucher = result.get("df_voucher_list")
        if df_voucher is not None and not df_voucher.empty:
            # Lọc đúng orderNo
            df_filtered = df_voucher[df_voucher["orderNo"]
                                     == extended_order_no]
            if not df_filtered.empty:
                voucher_list = []
                for i, row in df_filtered.iterrows():
                    voucher_code = row.get("decryptedPin", "")
                    coupon_href = row.get(
                        "pinUrl", "https://thammyvienngocdung.com/coupon-code/")
                    voucher_list.append({
                        "voucher_code": voucher_code,
                        "coupon_href": coupon_href
                    })

                # Gửi email với toàn bộ pinNo của line_item
                send_voucher_email(
                    customer_email=customer_email,
                    extended_order_no=extended_order_no,
                    goodsId=goodsId,
                    quantity=quantity,
                    voucher_list=voucher_list,
                    title=title,
                    product_image=product_image
                )

                # Lưu thông tin voucher (nếu cần cho log)
                for voucher in voucher_list:
                    all_voucher_info.append({
                        "extended_order_no": extended_order_no,
                        "goodsId": goodsId,
                        "title": title,
                        "quantity": quantity,
                        "decryptedPin": voucher.get("voucher_code", ""),
                        "coupon_href": voucher.get("coupon_href", "")
                    })
            else:
                all_voucher_info.append({
                    "extended_order_no": extended_order_no,
                    "goodsId": goodsId,
                    "title": title,
                    "quantity": quantity,
                    "decryptedPin": "No code found (orderNo mismatch)"
                })
        else:
            all_voucher_info.append({
                "extended_order_no": extended_order_no,
                "goodsId": goodsId,
                "title": title,
                "quantity": quantity,
                "decryptedPin": "No code (empty df_voucher)"
            })

    print("Asynchronous Voucher Processing Completed:", {
        "customer_email": customer_email,
        "voucher_details": all_voucher_info
    })


def col_num_to_letters(n):
    """Chuyển số cột (1-indexed) thành định dạng chữ (Excel: A, B, ..., AA, AB, ...)."""
    result = ""
    while n:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result
