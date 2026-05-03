import asyncio
import math

from prefect import flow, get_run_logger, task

from config.general import TRANSACTION_SUBCATEGORIES
from database.connection import get_conn
from llm.provider.openai import OpenAIProvider
from notifications import notify_on_completion, record_flow_result

BATCH_SIZE = 30
MINIMUM_CONFIDENCE = 0.5

def prepare_for_llm(tx):
    # Mapping of long DB keys to short LLM-friendly keys
    mapping = {
        'id': 'id',
        'source': 'src',
        'currency': 'cur',
        'amount': 'amt',
        'description': 'desc',
        'merchant': 'merch',
        'payee': 'payee',
        'transaction_type': 'type',
        'transaction_detail': 'det',
        'is_internal': 'internal'
    }
    
    # Create new dict with shortened keys, only if value exists and isn't 0/None
    # Note: we keep 'amt' even if 0, and 'internal' only if it's 1
    return {
        mapping[k]: v for k, v in tx.items() 
        if k in mapping and (k == 'amount' or v not in [None, 0, 0.0, ""])
    }


def get_batches(items, batch_size: int = BATCH_SIZE):
    """Yield successive n-sized chunks from items."""
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]

VALID_PAIRS = {
    (cat, sub)
    for cat, subs in TRANSACTION_SUBCATEGORIES.items()
    for sub in subs
}

def validate_classifications(classified_data: list[dict]) -> tuple[list, list]:
    valid, rejected = [], []
    for item in classified_data:
        pair = (item.get('category'), item.get('sub_category'))
        if pair in VALID_PAIRS:
            valid.append(item)
        else:
            item['category'] = 'Miscellaneous'
            item['sub_category'] = 'Uncategorised'
            item['confidence'] = 0.0
            rejected.append(item)
    return valid, rejected

@task
def categorise_internal_transactions():
    conn = get_conn()
    with conn:
        conn.execute("""
            UPDATE transactions
            SET category = 'Transfers',
                sub_category = 'Currency Conversion',
                confidence = 1.0
            WHERE is_internal = 1
            AND transaction_detail = 'EXCHANGE'
            AND category IS NULL
        """)
        conn.execute("""
            UPDATE transactions
            SET category = 'Transfers',
                sub_category = 'Internal Transfer',
                confidence = 1.0
            WHERE is_internal = 1
            AND (transaction_detail != 'EXCHANGE' OR transaction_detail IS NULL)
            AND category IS NULL
        """)
@task
def get_uncategorised_transactions():
    conn = get_conn(read_only=True)
    rows = conn.execute("""
        SELECT id, source, bank, amount,
            currency, description, payee,
            payment_reference, payer, fees, merchant,
            is_internal, is_interest, transaction_type,
            transaction_detail 
        FROM transactions
        WHERE is_internal = 0 AND 
        state != 'PENDING' AND
        (category IS NULL OR confidence <= ?);""", (MINIMUM_CONFIDENCE,)).fetchall()
    transactions = []
    for row in rows:
        transactions.append(dict(**row))
    return transactions

@task
def update_transaction_categories(classified_data):
    # Prepare the data: we need to flatten the nested dicts into a list of tuples
    # Format: (category, sub_category, transaction_id)
    logger = get_run_logger()
    conn = get_conn()
    update_payload = [
        (
            item.get('category'), 
            item.get('sub_category', None), 
            item.get('confidence'),
            item.get('id'),
            item.get('src'),
            item.get('cur'),
        )
        for item in classified_data
    ]

    sql = """
        UPDATE transactions 
        SET category = ?,
        sub_category = ?,
        confidence = ?
        WHERE id = ? AND source = ? AND currency = ?
    """

    # Use executemany to run the entire batch in one go
    with conn:
        conn.executemany(sql, update_payload)

    logger.info(f"Successfully updated {len(update_payload)} transactions.")
    return len(update_payload)


@flow(name="Categorise Transactions", on_failure=[notify_on_completion])
async def categorise_transactions_flow():
    logger = get_run_logger()

    llm = OpenAIProvider()

    categorise_internal_transactions()
    transactions = get_uncategorised_transactions()

    token_transactions = list(map(prepare_for_llm, transactions))

    batches = get_batches(token_transactions, BATCH_SIZE)
    total_batches = math.ceil(len(token_transactions) / BATCH_SIZE)
    valid_txns = 0
    reject_txns = 0
    for i, batch in enumerate(batches):
        try:
            llm_response = await llm.classify_transactions(batch)
            batch_data = llm_response.get("contents", [])

            sent_ids = {tx['id'] for tx in batch}
            returned_ids = {item['id'] for item in batch_data}
            missing = sent_ids - returned_ids
            if missing:
                logger.warning(f"Batch {i+1}: LLM returned {len(batch_data)}/{len(batch)} results. Missing IDs: {missing}")

            if isinstance(batch_data, list):
                logger.info(f"Succesfully categorised batch {i+1}/{total_batches}")
            else:
                logger.warning(f"Warning, batch failed to parse: {result.get("error", "Unknown error")}")
                continue
                
            valid, rejected = validate_classifications(batch_data)
            if rejected:
                logger.warning(f"Some transactions produced invalid category-subcategory pairs.")
                reject_txns += update_transaction_categories(rejected)
            valid_txns += update_transaction_categories(valid)

        except Exception as e:
            logger.error(f"Batch {i+1} LLM call failed: {e}")
            continue
    result = {"categorised": valid_txns, "rejected": reject_txns}
    record_flow_result(result)
    return result