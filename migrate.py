"""Explicit idempotent schema preparation: python migrate.py."""
from database import init_db, connect_db
from product_data import repair_cached_image_provenance

if __name__ == '__main__':
    init_db()
    db = connect_db()
    try:
        changed = repair_cached_image_provenance(db)
        db.commit()
        print(f"Legacy cached-image evidence corrected: {changed}")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
