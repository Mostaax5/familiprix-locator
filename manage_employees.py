"""Bootstrap the first store owner locally; no public setup endpoint or email.

Run python migrate.py first. Password input uses getpass, or --password-stdin
for a deployment secret pipe. Passwords are never command-line arguments.
"""
import argparse
import getpass
import sys
from database import connect_db, ensure_auth_schema
from employee_accounts import save_account


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('login')
    parser.add_argument('--name', required=True)
    parser.add_argument('--password-stdin', action='store_true')
    args = parser.parse_args()
    password = sys.stdin.readline().rstrip('\r\n') if args.password_stdin else getpass.getpass('Mot de passe du propriétaire: ')
    db = connect_db()
    try:
        ensure_auth_schema(db)
        owner = save_account(db, {'login': args.login, 'display_name': args.name,
                                  'role': 'owner', 'password': password}, bootstrap=True)
        db.commit()
        print('Compte propriétaire créé: ' + owner['login'])
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == '__main__':
    main()
