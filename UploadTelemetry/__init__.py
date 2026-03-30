import logging
import json
import os
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('=== Telemetry Upload Request Received ===')
    conn = None

    try:
        try:
            import pyodbc
        except ImportError as import_error:
            return func.HttpResponse(
                json.dumps({"success": False, "error": f"pyodbc import failed: {str(import_error)}"}),
                status_code=500,
                mimetype="application/json"
            )

        data = req.get_json()
        adid = data.get('adid')

        if not adid:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "ADID is required"}),
                status_code=400,
                mimetype="application/json"
            )

        logging.info(f'Uploading telemetry for ADID: {adid}')

        total_suggestions = data.get('totalSuggestions', 0)
        total_accepted = data.get('totalAccepted', 0)
        total_rejected = data.get('totalRejected', 0)
        acceptance_rate = data.get('acceptanceRate', 0)
        lines_saved = data.get('linesSaved', 0)
        characters_saved = data.get('charactersSaved', 0)
        session_count = data.get('sessionCount', 0)
        first_usage = data.get('firstUsage')
        last_usage = data.get('lastUsage')
        language_stats = data.get('languageStats')
        client_timestamp = data.get('timestamp')

        language_stats_json = json.dumps(language_stats) if language_stats else None

        connection_string = os.environ.get('SqlConnectionString')
        
        if not connection_string:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Database not configured"}),
                status_code=500,
                mimetype="application/json"
            )

        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        cursor.execute("""
            EXEC sp_UploadCopilotTelemetry 
                @ADID=?, @TotalSuggestions=?, @TotalAccepted=?, @TotalRejected=?, 
                @AcceptanceRate=?, @LinesSaved=?, @CharactersSaved=?, @SessionCount=?,
                @FirstUsageDate=?, @LastUsageDate=?, @LanguageStatsJson=?, @ClientTimestamp=?
        """, adid, total_suggestions, total_accepted, total_rejected, acceptance_rate,
            lines_saved, characters_saved, session_count, first_usage, last_usage,
            language_stats_json, client_timestamp)
        
        row = cursor.fetchone()

        if row:
            conn.commit()
            result = {
                "success": True,
                "status": row.Status,
                "telemetryId": row.TelemetryId,
                "adid": row.ADID,
                "message": "Telemetry data uploaded successfully"
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
