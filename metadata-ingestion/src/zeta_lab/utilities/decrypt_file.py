import os
import click
from Crypto.Cipher import AES
import binascii

class DecryptFile:
    SESSION_KEY = binascii.unhexlify("f4150d4a1ac5708c29e437749045a39a")

    def __init__(self, home_path=None):
        self.cipher = AES.new(self.SESSION_KEY, AES.MODE_ECB)
        self.home_path = home_path or os.environ.get('LIAENG_HOME', '')

    @staticmethod
    def pad_byte(src):
        block_size = 16
        padding_size = block_size - (len(src) % block_size)
        padding = bytes([padding_size] * padding_size)
        return src + padding

    @staticmethod
    def unpad_byte(src):
        padding_size = src[-1]
        return src[:-padding_size]

    @staticmethod
    def hex2byte(hex_str):
        return binascii.unhexlify(hex_str)

    @staticmethod
    def to_hex_string(bytes_arr):
        return binascii.hexlify(bytes_arr).decode('ascii').lower()

    def encrypt(self, plain):
        padded = self.pad_byte(plain.encode('utf-8'))
        return self.cipher.encrypt(padded)

    def decrypt(self, encrypted):
        decrypted = self.cipher.decrypt(encrypted)
        return self.unpad_byte(decrypted).decode('utf-8')

    def encrypt_str(self, val):
        if val is None:
            return ""
        encrypt = self.encrypt(val)
        return self.to_hex_string(encrypt)

    def decrypt_str(self, val):
        if not val:
            return ""
        decrypt = self.decrypt(self.hex2byte(val))
        return decrypt

    def encrypt_security(self, id_str, pw_str):
        id_encrypted = self.encrypt_str(id_str)
        pw_encrypted = self.encrypt_str(pw_str)

        encryption_id_pw = f"{id_encrypted}\\|{pw_encrypted}"
        encryption_id_pw = self.encrypt_str(encryption_id_pw)
        click.echo(f"ID/Password : {encryption_id_pw}")

        with open(os.path.join(self.home_path, "config", "security.properties"), "w") as fw:
            fw.write(encryption_id_pw)

    def decrypt_security(self):
        id_plain, pw_plain = self.get_decrypted_credentials()
        if id_plain and pw_plain:
            click.echo(f"Decrypted ID       : {id_plain}")
            click.echo(f"Decrypted Password : {pw_plain}")
        else:
            click.echo("Failed to decrypt credentials")

    @classmethod
    def get_decrypted_credentials(cls, security_path=None):
        if not security_path:
            raise ValueError("Please provide security.properties path")

        if os.path.exists(security_path):
            with open(security_path, "r") as bfr:
                encrypted = bfr.readline().strip()
                cipher = AES.new(cls.SESSION_KEY, AES.MODE_ECB)
                decrypted = cls.unpad_byte(cipher.decrypt(cls.hex2byte(encrypted))).decode('utf-8')
                temp_index = decrypted.find("\\|")
                if temp_index != -1:
                    encrypted_id = decrypted[:temp_index]
                    encrypted_pw = decrypted[temp_index+2:]
                    decrypted_id = cls.unpad_byte(cipher.decrypt(cls.hex2byte(encrypted_id))).decode('utf-8')
                    decrypted_pw = cls.unpad_byte(cipher.decrypt(cls.hex2byte(encrypted_pw))).decode('utf-8')
                    return decrypted_id, decrypted_pw
                else:
                    click.echo("Invalid format in security file")
        else:
            click.echo("Security file does not exist")
        return None, None

@click.group()
@click.option('--home', default=None, help='Path to LIAENG_HOME. If not provided, uses LIAENG_HOME environment variable.')
@click.pass_context
def cli(ctx, home):
    """DecryptFile CLI"""
    ctx.obj = DecryptFile(home)

@cli.command()
@click.argument('id')
@click.argument('password')
@click.pass_obj
def enc(decrypt_file, id, password):
    """Encrypt ID and password"""
    if len(password) > 15:
        click.echo("Please check your password again. Password is too long...(maximum length 15 characters)")
    else:
        decrypt_file.encrypt_security(id, password)

@cli.command()
@click.pass_obj
def dec(decrypt_file):
    """Decrypt ID and password from file"""
    decrypt_file.decrypt_security()

@cli.command()
@click.option('--home', default=None, help='Path to LIAENG_HOME. If not provided, uses LIAENG_HOME environment variable.')
def get_credentials(home):
    """Get decrypted ID and password from file"""
    id_plain, pw_plain = DecryptFile.get_decrypted_credentials(home)
    if id_plain and pw_plain:
        click.echo(f"Decrypted ID       : {id_plain}")
        click.echo(f"Decrypted Password : {pw_plain}")
    else:
        click.echo("Failed to get credentials")

if __name__ == "__main__":
    cli()