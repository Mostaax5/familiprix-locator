"""Generate a production password hash without exposing the password to GitHub."""

from getpass import getpass
import sys

from werkzeug.security import generate_password_hash

from security import password_validation_error


def main() -> int:
    password = getpass("New store passphrase: ")
    validation_error = password_validation_error(password)
    if validation_error:
        print(validation_error, file=sys.stderr)
        return 1
    if password != getpass("Confirm passphrase: "):
        print("The passphrases do not match.", file=sys.stderr)
        return 1
    print(generate_password_hash(password, method="scrypt:32768:8:1", salt_length=32))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
