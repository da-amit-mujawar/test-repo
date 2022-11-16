create or replace function GetMatchCode(
FirstName varchar(max),
LastName varchar(max),
Address varchar(max),
Zip varchar(max),
Company varchar(max),
MatchCodeType varchar(max))
returns varchar(max)
stable
as $$


def format_match_string(str_or_none):
    if str_or_none is None:
        return "NULL"
    else:
        return str_or_none.upper().strip()

def StandardizeZip1(objZip):
    if objZip is None :
        objZip = 'NULL'
    strZip = str(objZip)

    if len(objZip) < 5:
        objZip = strZip.ljust(5, '*')
    else:
        objZip = strZip[0:5]
    return objZip


def StandardizeBusinessPortion1(lsCompany):

    objWord1 = ''
    objWord2 = ''
    objCompany = ''

    lsCompany = lsCompany.replace(".", '').replace("'", '').replace(",", '').replace("&", '')

    sCompany = str(lsCompany).split()

    if len(sCompany) > 0:
        objWord1 = sCompany[0]

    if len(sCompany) > 1:
        objWord2 = sCompany[1]

    if len(objWord2) == 0:
        if len(objWord1) > 0:
            strWord1 = str(objWord1)
            if len(strWord1) >= 4:
                objWord1 = strWord1[:4]
    else:
        strWord1 = str(objWord1)
        strWord2 = str(objWord2)

        if len(strWord1) < 2:
            objWord1 = objWord1 + "*"
        else:
            if len(strWord1) > 2:
                objWord1 = strWord1[:2]
        if len(strWord2) < 2:
            objWord2 = objWord2 + "*"
        else:
            if len(strWord2) > 2:
                objWord2 = strWord2[:2]

    objCompany = objCompany + objWord1 + objWord2
    if len(objCompany) < 4:
        objCompany = objCompany.ljust(4, '*')

    return objCompany


def StandardizeFirstName1(lsFirstName):
    lsFirstName = lsFirstName.replace(".", '').replace("'", '').replace(",", '').replace(" ", '')
    objVowel = ''
    objFirstName = ''

    if len(lsFirstName) > 1:
        objFirstName = lsFirstName[0]
    else:
        objFirstName = lsFirstName

    iFirstCounter = 1

    while iFirstCounter < len(lsFirstName) and len(objFirstName) < 3:
        sChar = lsFirstName[iFirstCounter]
        sCharToUpper = sChar.upper()
        if sCharToUpper == "A" or sCharToUpper == "E" or sCharToUpper == "I" or sCharToUpper == "O" or sCharToUpper == "U":
            objVowel = objVowel + sChar
        else:
            objFirstName = objFirstName + sChar
        iFirstCounter = iFirstCounter + 1

    if len(objFirstName) < 3:
        if len(objVowel) >= 3 - len(objFirstName):
            objFirstName = objFirstName + objVowel[0: 3 - len(objFirstName)]
        else:
            objFirstName = objFirstName + objVowel

    if len(objFirstName) < 3:
        objFirstName = objFirstName.ljust(3, '*')

    return objFirstName


def StandardizeCommonNames1(sFirstName):
    lsFirstName = ''
    if sFirstName in ('ALAN', 'ALEXANDER', 'ALLAN', 'ALLEN'):
        lsFirstName = 'AL'
    elif sFirstName == "ANTHONY":
        lsFirstName = "TONY"
    elif sFirstName == "CHRISTINA":
        lsFirstName = "TINA"
    elif sFirstName in ("EDMOND", "EDMUND", "EDUARDO", "EDWARD"):
        lsFirstName = "ED"
    elif sFirstName == "ELIZABETH":
        lsFirstName = "LIZ"
    elif sFirstName == "GEOFFREY":
        lsFirstName = "JEFF"
    elif sFirstName == "JACOB":
        lsFirstName = "JAKE"
    elif sFirstName == "JAMES":
        lsFirstName = "JIM"
    elif sFirstName == "JOSEPH":
        lsFirstName = "JOE"
    elif sFirstName in ("LAURENCE", "LAWRENCE"):
        lsFirstName = "LARRY"
    elif sFirstName == "MARGARET":
        lsFirstName = "MAGGY"
    elif sFirstName == "MICHAEL":
        lsFirstName = "MIKE"
    elif sFirstName == "SUSAN":
        lsFirstName = "SUE"
    elif sFirstName == "THEODORE":
        lsFirstName = "TED"
    elif sFirstName == "THOMAS":
        lsFirstName = "TOM"
    else:
        lsFirstName = sFirstName

    return lsFirstName


def StandardizeLastName1(lsLastName):
    objVowel = ''
    objName = ''
    lsLastName = lsLastName.replace(".", '').replace("'", '').replace(",", '').replace(" ", '')

    if len(lsLastName) > 0:
        objName = lsLastName[0]

    iNameCounter = 1
    while iNameCounter < len(lsLastName) and len(objName) < 3:
        sChar = lsLastName[iNameCounter]
        sCharToUpper = sChar.upper()
        if sCharToUpper == "A" or sCharToUpper == "E" or sCharToUpper == "I" or sCharToUpper == "O" or sCharToUpper == "U":
            objVowel = objVowel + sChar
        else:
            objName = objName + sChar

        iNameCounter = iNameCounter + 1

    if len(objName) < 3:
        if len(objVowel) >= 3 - len(objName):
            objName = objName + objVowel[0:3 - len(objName)]

        else:
            objName = objName + objVowel

    if len(objName) < 3:
        objName = objName.ljust(3, '*')

    return objName

def udf_replacemultiplestring(input_str, toreplace_str, replacewith_str):
    for elem in toreplace_str:
        if elem in input_str:
            input_str = input_str.replace(elem, replacewith_str)
    return input_str

def udf_isnumeric(input_str):
    intresult = 0
    input_str = str(input_str)
    input_str = udf_replacemultiplestring(input_str, ['.', '-', '+'], '').strip()
    if input_str.isdigit():
        intresult = 1
    return intresult


def StandardizeAddress1(lsAddress):
    objVowel = ''
    objConsonant = ''
    objNumbers = ''
    objStreet = ''

    lsAddress = lsAddress.replace("-", '').replace(".", '').replace(",", '').replace("#", '')

    if len(lsAddress) > 0:
        sFirstChar = str(lsAddress[0])

        if udf_isnumeric(sFirstChar):
            bNaN = True
            objStreet = objStreet + sFirstChar

            iCount = 1
            while iCount <= 3 and bNaN and iCount < len(lsAddress):
                if udf_isnumeric(lsAddress[iCount]):
                    objStreet = objStreet + lsAddress[iCount]
                    iCount = iCount + 1
                else:
                    bNaN = False

            iNextSpace = len(objStreet)
            if len(objStreet) < len(lsAddress):
                iNextSpace = lsAddress.find(' ', len(objStreet), len(lsAddress))
                if iNextSpace == -1: iNextSpace = len(lsAddress)

            if len(objStreet) < 4:
                objStreet = objStreet.ljust(4, '*')

            while iNextSpace < len(lsAddress) and len(objStreet) < 6:
                sChar = str(lsAddress[iNextSpace])
                iNextSpace = iNextSpace + 1

                encodedBytes = ord(sChar)
                if not ((65 <= encodedBytes <= 90) or (97 <= encodedBytes <= 122)):
                    continue

                if len(objStreet) == 4:
                    objStreet = objStreet + sChar
                    continue

                if sChar.upper() in ('A', 'E', 'I', 'O', 'U'):
                    objVowel = objVowel + sChar
                else:
                    if len(objStreet) == 5:
                        objStreet = objStreet + sChar

            if len(objStreet) < 6:
                if len(objVowel) > 6 - len(objStreet):
                    objStreet = objStreet + objVowel[:6 - len(objStreet)]
                else:
                    objStreet = objStreet + objVowel

        else:
            if len(objStreet) > 1:
                objStreet = objStreet.replace(str(objStreet), str(objStreet)[0])
            iCounter = 1

            while iCounter < len(lsAddress) and (len(objNumbers) < 4 or len(objConsonant) < 2):

                sChar = str(lsAddress[iCounter])
                iCounter = iCounter + 1

                iDigit = udf_isnumeric(sChar)

                encodedBytes = ord(sChar)
                if not ((65 <= encodedBytes <= 90) or (97 <= encodedBytes <= 122) or iDigit):
                    continue

                if iDigit:
                    objNumbers = objNumbers + sChar
                else:
                    sCharToUpper = sChar.upper()
                    if sCharToUpper == "A" or sCharToUpper == "E" or sCharToUpper == "I" or sCharToUpper == "O" or sCharToUpper == "U":
                        objVowel = objVowel + sChar
                    else:
                        objConsonant = objConsonant + sChar

            if len(objNumbers) >= 4:
                iSubStringLength = 4
            else:
                iSubStringLength = len(objNumbers)

            objStreet = objNumbers[0:iSubStringLength]

            if len(objStreet) < 4:
                objStreet = objStreet.ljust(4, '*')

            objStreet = objStreet + sFirstChar

            if len(objConsonant) > 1:
                objStreet = objStreet + str(objConsonant[0])
            else:
                objStreet = objStreet + str(objConsonant)

            if len(objStreet) < 6:
                if len(str(objVowel)) >= 6 - len(objStreet):
                    objStreet = objStreet + objVowel[0: 6 - len(objStreet)]
                else:
                    objStreet = objStreet + objVowel

    if len(objStreet) < 6:
        objStreet = objStreet.ljust(6, '*')
    return objStreet


def null_to_empty(s):
    if s is None:
        return ''
    return str(s.strip())

strMatchCodeType = str(MatchCodeType).strip().upper()
sValue = ''

try:
  if strMatchCodeType == "C" or strMatchCodeType == "I":
      lsAddress = format_match_string(Address)
      objStreet = ''

      lsLastName = format_match_string(LastName)
      objName = ''

      lsFirstName = format_match_string(FirstName)
      objFirstName = ''

      lsCompany = format_match_string(Company)
      objCompany = ''

      lsZip = format_match_string(Zip)


      objStreet = StandardizeAddress1(lsAddress)

      if strMatchCodeType == "I":

          objName = StandardizeLastName1(lsLastName)
          lsFirstName = StandardizeCommonNames1(lsFirstName.upper())
          objFirstName = StandardizeFirstName1(lsFirstName)

      elif strMatchCodeType == "C":
          objCompany = StandardizeBusinessPortion1(lsCompany)
      objZip = StandardizeZip1(lsZip)
      if strMatchCodeType == "I":
          sValue = objZip + objStreet + objName + objFirstName
      else:
          sValue = objZip + objStreet + objCompany
  return sValue
except:
  return 'ERROR'

$$ language plpythonu;

