from flask_login import UserMixin
from models import User, ROLE_ADMIN, ROLE_LAB, ROLE_OFFICER


class LoginUser(UserMixin):
    def __init__(self, user):
        self.id = user.id
        self.role = user.role
        self.email = user.email
        self.name = user.name

    @property
    def is_admin(self):
        return self.role == ROLE_ADMIN

    @property
    def is_lab(self):
        return self.role == ROLE_LAB

    @property
    def is_officer(self):
        return self.role == ROLE_OFFICER
