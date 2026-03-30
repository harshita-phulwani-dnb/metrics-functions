import logging
import json
import os
import pyodbc
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('=== User Registration Request Received ===')
    conn = None

    try:
        req_body = req.get_json()
        adid = req_body.get('adid')
        email = req_body.get('email')

        if not adid:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "ADID is required"}),
                status_code=400,
                mimetype="application/json"
            )

        if not email:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Email is required"}),
                status_code=400,
                mimetype="application/json"
            )

        logging.info(f'Registering user: ADID={adid}, Email={email}')

        connection_string = os.environ.get('SqlConnectionString')
        
        if not connection_string:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Database not configured"}),
                status_code=500,
                mimetype="application/json"
            )

        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("EXEC sp_RegisterCopilotUser @ADID=?, @Email=?", adid, email)
        row = cursor.fetchone()

        if row:
            conn.commit()
            result = {
                "success": True,
                "status": row.Status,
                "adid": row.ADID,
                "email": row.Email,
                "internalId": row.InternalId,
                "message": "User registered successfully" if row.Status == "registered" else "User record updated"
            }
        else:
            conn.rollback()
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Stored procedure returned no data"}),
                status_code=500,
                mimetype="application/json"
            )

        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f'Error: {str(e)}')
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass