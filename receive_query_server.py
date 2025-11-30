from flask import Flask, request, jsonify
import psycopg2
import datetime
from p2i_postgres import execute_query_transactionally_atomic  # 새로운 트랜잭션 처리 함수



app = Flask(__name__)
@app.route("/receive-query", methods=["POST"])
def run_transaction():
    try:
        data = request.get_json(force=True)
        print("📥 받은 데이터:", data)
        if not isinstance(data, list):
            return jsonify({"status": "error", "reason": "expected list of [sql, args]"}), 400

        for i, stmt in enumerate(data):
            if not isinstance(stmt, list) or len(stmt) != 2:
                print(f"❌ Statement[{i}] 형식 오류:", stmt)
                return jsonify({"status": "error", "reason": f"Invalid format in statement {i}"}), 400
            sql, params = stmt
            if not isinstance(sql, str) or not isinstance(params, list):
                print(f"❌ Statement[{i}] 내부 타입 오류: sql={sql}, params={params}")
                return jsonify({"status": "error", "reason": f"Invalid types in statement {i}"}), 400

    except Exception as e:
        print("❌ 예외 발생:", e)
        raw = request.data.decode('utf-8', errors='replace')
        print("❌ 원문 바이트 → 문자열:", raw)
        return jsonify({"status": "error", "reason": f"invalid JSON: {e}"}), 400

    # ✅ 검증 완료 → 실제 트랜잭션 실행
    result = execute_query_transactionally_atomic(data)

    if result["status"] == "ok":
        return jsonify({"status": "ok", "executed": result["executed"], "results": result["results"]})
    else:
        return jsonify({"status": "error", "message": result["message"], "executed": result["executed"]}), 500





if __name__ == "__main__":
    app.run(port=5000, threaded=False, processes=1)
