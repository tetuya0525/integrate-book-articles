# ==============================================================================
# Memory Library - Integrate Book Articles Service
# Role:         Moves processed articles from staging to the final collection.
# Version:      1.0 (Flask Architecture)
# Author:       心理 (Thinking Partner)
# Last Updated: 2025-07-11
# ==============================================================================
import os
import base64
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import firestore
import logging

# Pythonの標準ロギングを設定
logging.basicConfig(level=logging.INFO)

# Flaskアプリケーションを初期化
app = Flask(__name__)

# Firestoreクライアントの遅延初期化
db = None

def get_firestore_client():
    """Firestoreクライアントをシングルトンとして取得・初期化する"""
    global db
    if db is None:
        try:
            firebase_admin.initialize_app()
            db = firestore.client()
            app.logger.info("Firebase app initialized successfully.")
        except Exception as e:
            app.logger.error(f"Error initializing Firebase app: {e}")
    return db

@app.route('/', methods=['POST'])
def integrate_article():
    """
    Pub/Subからのプッシュ通知を受け取り、記事を正式な書庫に移動させる。
    """
    db_client = get_firestore_client()
    if not db_client:
        app.logger.error("Firestore client not initialized. Cannot process message.")
        return "Internal Server Error", 500

    # Pub/Subからのリクエストボディを取得
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        app.logger.error(f"Bad Pub/Sub request: {envelope}")
        return "Bad Request: invalid Pub/Sub message format", 400

    # メッセージデータをデコード
    try:
        message = envelope['message']
        doc_id = base64.b64decode(message['data']).decode('utf-8').strip()
        app.logger.info(f"Received task to integrate document: {doc_id}")
    except Exception as e:
        app.logger.error(f"Failed to decode Pub/Sub message: {e}")
        return "Bad Request: could not decode message data", 400

    # トランザクション内で処理を行い、データの整合性を保証する
    transaction = db_client.transaction()
    staging_doc_ref = db_client.collection('staging_articles').document(doc_id)
    final_doc_ref = db_client.collection('articles').document(doc_id)

    try:
        @firestore.transactional
        def move_document(trans, staging_ref, final_ref):
            # 1. stagingからドキュメントを取得
            staging_doc = staging_ref.get(transaction=trans)
            if not staging_doc.exists:
                app.logger.warning(f"Document {doc_id} no longer exists in staging. Already processed?")
                return # 何もせず終了

            if staging_doc.to_dict().get('status') != 'processed':
                app.logger.warning(f"Document {doc_id} is not in 'processed' state. Ignoring.")
                return # 処理対象でないので終了

            # 2. articlesに新しいドキュメントを作成
            doc_data = staging_doc.to_dict()
            # 不要になったstatusフィールドを削除
            doc_data.pop('status', None) 
            trans.set(final_ref, doc_data)
            app.logger.info(f"Copied document {doc_id} to 'articles' collection.")

            # 3. stagingから元のドキュメントを削除
            trans.delete(staging_ref)
            app.logger.info(f"Deleted document {doc_id} from 'staging_articles' collection.")

        # トランザクションを実行
        move_document(transaction, staging_doc_ref, final_doc_ref)
        
        app.logger.info(f"Successfully integrated document {doc_id}.")
        # Pub/Subにメッセージの処理成功を伝える (ACK)
        return "Success", 204

    except Exception as e:
        app.logger.error(f"Failed to integrate document {doc_id} in transaction: {e}")
        # 不明なエラーが発生した場合、再試行を期待してエラーを返す
        return "Internal Server Error", 500
