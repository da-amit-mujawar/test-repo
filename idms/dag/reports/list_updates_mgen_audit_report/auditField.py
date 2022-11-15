class AuditField:

    def __init__(self, tableName: str, fieldName: str, fieldType: str):
        self.tableName = tableName
        self.fieldName = fieldName
        self.fieldType = fieldType if fieldType else "S"

    def __str__(self):
        return self.tableName + ', ' + self.fieldName + ', ' + self.fieldType

    def __eq__(self, other):
        if isinstance(other, AuditField):
            return self.__key() == other.__key()
        else:
            return false

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return self.tableName, self.fieldName, self.fieldType
