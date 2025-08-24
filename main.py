# ==============================================================================
# Memory Library - Integrate Book Articles Service
# main.py (v2.1 Final)
#
# Role:         処理済み記事をステージングから最終的な書庫へ移動させる。
#               重大な問題を修正し、堅牢性を確保した最終版。
# Version:      2.1
# Last Updated: 2025-08-24
# ==============================================================================
import os
import json
import logging
import base64
from typing import Dict, Any, Optional, Protocol
from dataclasses import dataclass
from enum import Enum
import backoff

from flask import Flask, request, jsonify, current_app
import firebase_admin
from firebase_admin import firestore
from google.api_core import exceptions as gcp_exceptions

# --- 定数と設定 ---
class DocumentStatus(Enum):
    PROCESSED = 'processed'

@dataclass
class Config:
    STAGING_COLLECTION: str = "staging_articles"
    FINAL_COLLECTION: str = "articles"
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")

# --- エラーとバリデーション ---
class ValidationError(Exception): pass

class Validator:
    @staticmethod
    def validate_document_id(doc_id: Optional[str]) -> str:
        # ★ [Claude] 入力バリデーションを強化 
        if not doc_id or not isinstance(doc_id, str):
            raise ValidationError("documentIdが無効です")
        
        stripped_id = doc_id.strip()
        if not stripped_id or len(stripped_id) > 100:
            raise ValidationError("documentIdの形式が不正です")
        
        if '/' in stripped_id or stripped_id.startswith('__'):
            raise ValidationError("documentIdに無効な文字が含まれています")
            
        return stripped_id

    @staticmethod
    def validate_document(doc_data: Optional[Dict[str, Any]], doc_id: str):
        if not doc_data:
            raise ValidationError(f"ドキュメントが存在しません: {doc_id}")
        if doc_data.get('status') != DocumentStatus.PROCESSED.value:
            raise ValidationError(f"ドキュメントは処理対象外です (status: {doc_data.get('status')})")

# --- 依存関係の抽象化 ---
class FirestoreClientProtocol(Protocol):
    def collection(self, name: str) -> Any: ...
    def transaction(self) -> Any: ...

# --- ビジネスロジック層 ---
class ArticleIntegrator:
    # (ロジックはv2.0と同様)
    def __init__(self, db: FirestoreClientProtocol, config: Config): self.db = db; self.config = config
    def _prepare_final_doc(self, data: Dict[str, Any]) -> Dict[str, Any]:
        final_data = data.copy()
        for field in ['status', 'queuedAt', 'processingStartedAt', 'processedAt', 'batchId']: final_data.pop(field, None)
        final_data['integratedAt'] = firestore.SERVER_TIMESTAMP
        final_data['updatedAt'] = firestore.SERVER_TIMESTAMP
        return final_data

    # ★ [Claude] リトライ対象のエラーを具体的にし、リトライ時のログを追加 [cite: 5546-5555]
    @backoff.on_exception(backoff.expo,
                          (gcp_exceptions.Aborted, gcp_exceptions.DeadlineExceeded, gcp_exceptions.ServiceUnavailable),
                          max_tries=3,
                          on_backoff=lambda details: log_structured(
                              'WARNING', f"リトライします (試行 {details['tries']}回目)", 
                              document_id=details['args'][0] if details.get('args') else 'unknown',
                              error=details['exception']
                          ))
    def integrate(self, doc_id: str) -> bool:
        staging_ref = self.db.collection(self.config.STAGING_COLLECTION).document(doc_id)
        final_ref = self.db.collection(self.config.FINAL_COLLECTION).document(doc_id)
        transaction = self.db.transaction()
        @firestore.transactional
        def _integrate_in_transaction(trans):
            staging_snapshot = staging_ref.get(transaction=trans)
            final_snapshot = final_ref.get(transaction=trans)
            if final_snapshot.exists:
                if staging_snapshot.exists: trans.delete(staging_ref)
                return False
            Validator.validate_document(staging_snapshot.to_dict(), doc_id)
            final_doc = self._prepare_final_doc(staging_snapshot.to_dict())
            trans.set(final_ref, final_doc)
            trans.delete(staging_ref)
            return True
        return _integrate_in_transaction(transaction)

# --- アプリケーション層 (Flask) ---
# ★ [Claude, ChatGPT] log_structured関数の実装漏れという重大な問題を修正 
def log_structured(level: str, message: str, **kwargs: Any) -> None:
    log_data = {"message": message, "severity": level.upper(), **kwargs}
    log_level_map = {
        "INFO": logging.info, "WARNING": logging.warning,
        "ERROR": logging.error, "CRITICAL": logging.critical,
    }
    logger_func = log_level_map.get(level.upper(), logging.info)
    exc_info = kwargs.pop("exc_info", None)
    logger_func(json.dumps(log_data, ensure_ascii=False, default=str), exc_info=exc_info)

def create_app() -> Flask:
    """★ [Claude] アプリ初期化処理の不完全性を修正し、完全な実装を提供 """
    app = Flask(__name__)
    config = Config()
    app.config['APP_CONFIG'] = config
    
    logging.basicConfig(level=config.LOG_LEVEL, format='%(message)s')
    
    try:
        if not firebase_admin._apps:
            firebase_admin.initialize_app()
        app.db_client = firestore.client()
        app.integrator = ArticleIntegrator(app.db_client, config)
    except Exception as e:
        log_structured("CRITICAL", "アプリケーション初期化に失敗", error=str(e))
        raise

    @app.route('/', methods=['POST'])
    def handle_pubsub_message():
        envelope = request.get_json()
        if not envelope or 'message' not in envelope: return "Bad Request", 400
        try:
            pubsub_message = base64.b64decode(envelope['message']['data']).decode('utf-8')
            message_data = json.loads(pubsub_message)
            doc_id = Validator.validate_document_id(message_data.get('documentId'))

            integrator = current_app.integrator
            success = integrator.integrate(doc_id)
            
            if success:
                log_structured("INFO", "ドキュメントの統合に成功しました", document_id=doc_id)
                return "Success", 204
            else:
                log_structured("INFO", "ドキュメントは既に統合済みのためスキップしました", document_id=doc_id)
                return "Skipped", 200

        except ValidationError as e:
            log_structured('WARNING', "バリデーションエラー", error=str(e))
            return "Bad Request", 400
        except Exception as e:
            log_structured('ERROR', "予期せぬエラー", error=str(e), exc_info=True)
            return "Internal Server Error", 500

    @app.route('/status', methods=['GET'])
    def get_status():
        try:
            db_client = current_app.db_client
            # Note: count()は大規模コレクションではレイテンシが増加する可能性があるが、
            # 読み取りコストは1回分であり、現状のユースケースでは許容可能と判断。
            staging_count = db_client.collection(config.STAGING_COLLECTION).count().get()[0][0].value
            final_count = db_client.collection(config.FINAL_COLLECTION).count().get()[0][0].value
            return jsonify({"status": "ok", "staging_documents": staging_count, "final_documents": final_count})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 503
            
    return app

if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
