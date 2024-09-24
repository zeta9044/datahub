import os
import click
from Crypto.Cipher import AES
import binascii

class DecryptFile:
    """
        Class to handle file encryption and decryption using AES algorithm.
    """
    def __init__(self):
        self.session_key = binascii.unhexlify("f4150d4a1ac5708c29e437749045a39a")
        self.cipher = AES.new(self.session_key, AES.MODE_ECB)

    def encrypt(self, plain):
        """
        :param plain: The plain text string to be encrypted.
        :return: The encrypted byte string after padding and encryption.
        """
        padded = self.pad_byte(plain.encode('utf-8'))
        return self.cipher.encrypt(padded)

    def decrypt(self, encrypted):
        """
        :param encrypted: The encrypted data that needs to be decrypted.
        :return: The decrypted and unpadded string in UTF-8 format.
        """
        decrypted = self.cipher.decrypt(encrypted)
        return self.unpad_byte(decrypted).decode('utf-8')

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
        return binascii.hexlify(bytes_arr).decode('ascii')

    @classmethod
    def encrypt_str(cls, val):
        if val is None:
            return ""
        da = cls()
        encrypt = da.encrypt(val)
        return cls.to_hex_string(encrypt)

    @classmethod
    def decrypt_str(cls, val):
        if not val:
            return ""
        da = cls()
        decrypt = da.decrypt(cls.hex2byte(val))
        return decrypt

    @staticmethod
    def encrypt_security(id_str, pw_str):
        """
        :param id_str: The ID string to be encrypted.
        :param pw_str: The password string to be encrypted.
        :return: None
        """
        system_home = os.environ.get('LIAENG_HOME', '')
        id_encrypted = DecryptFile.encrypt_str(id_str)
        pw_encrypted = DecryptFile.encrypt_str(pw_str)

        encryption_id_pw = f"{id_encrypted}\\|{pw_encrypted.strip()}"
        encryption_id_pw = DecryptFile.encrypt_str(encryption_id_pw)
        click.echo(f"ID/Password : {encryption_id_pw}")

        with open(os.path.join(system_home, "config", "security.properties"), "w") as fw:
            fw.write(encryption_id_pw)

    @staticmethod
    def decrypt_security(gubun):
        """
        :param gubun: Specifies the type of information to be decrypted. Valid values are "ID" for user ID and "PW" for password.
        :return: Decrypted user ID or password based on the provided gubun parameter. Returns an empty string if the security file does not exist or if the gubun parameter is invalid.
        """
        system_home = os.environ.get('LIAENG_HOME', '')
        sf_path = os.path.join(system_home, "config", "security.properties")

        if os.path.exists(sf_path):
            with open(sf_path, "r") as bfr:
                temp = bfr.readline().strip()
                temp_index = temp.find("\\|")
                if temp_index == -1:
                    tmp_id_pw = DecryptFile.decrypt_str(temp)
                    temp_index = tmp_id_pw.find("\\|")
                    temp = tmp_id_pw
                temp_user = temp[:temp_index]
                temp_password = temp[temp_index+2:]

                if gubun == "ID":
                    return DecryptFile.decrypt_str(temp_user)
                elif gubun == "PW":
                    return DecryptFile.decrypt_str(temp_password)
        else:
            click.echo("Not exist Security File . . .")
        return ""

@click.group()
def cli():
    """DecryptFile CLI"""
    pass

@cli.command()
@click.argument('id')
@click.argument('password')
def encrypt(id, password):
    """Encrypt ID and password"""
    if len(password) > 15:
        click.echo("Please check your password again. Password is too long...(maximum length 15 characters)")
    else:
        DecryptFile.encrypt_security(id, password)

@cli.command()
@click.argument('id')
@click.argument('password')
def decrypt(id, password):
    """Decrypt ID and password"""
    click.echo(f"Input ID       : {id}")
    click.echo(f"Input Password : {password}")
    click.echo(f"Dec ID         : {DecryptFile.decrypt_security('ID')}")
    click.echo(f"Dec Password   : {DecryptFile.decrypt_security('PW')}")

@cli.command()
@click.argument('value')
def encrypt_value(value):
    """Encrypt a single value"""
    encrypted = DecryptFile.encrypt_str(value)
    click.echo(f"Encrypted: {encrypted}")

@cli.command()
@click.argument('value')
def decrypt_value(value):
    """Decrypt a single value"""
    decrypted = DecryptFile.decrypt_str(value)
    click.echo(f"Decrypted: {decrypted}")

if __name__ == "__main__":
    cli()