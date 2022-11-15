import re
import sys

class TitleSearch:
    """
    Requires:
    title - That will used to determine a code.
    """

    def __init__(self, title):
        self.title = title

    # Define `match()` function
    def match(self, expr):
        """ tries to match the provided expression "expr" in the title.

        Returns:
              True if it finds one otherwise False
        """
        # print(self.title, expr, re.match(expr, self.title))
        return re.search(expr, self.title, re.IGNORECASE)

    # Define `match_t()` function
    def match_t(self, expr_list):
        """ compares the title with each expression provided in the list of expressions.
        comparision stops as soon as it finds a match

        Returns:
              True if it finds one otherwise False
        """
        for i in expr_list:
            if i.lower() == self.title.lower():
                return 1
        return 0


# Define `get_title_code()` function
def get_title_code(ps_title):
    """Given a Title, this functions determines the appropriate title code assignment if it can
    otherwise return null.

    Returns:
        Title Code according to Data Axle norms
    """
    try:
        # convert case
        ls_title = ps_title.rstrip('\n').lower().strip()
        ls_replist = [(',', ''), ('#', ''), ('"', ''), ('\'', ''), ('.', ''), ('-', ' '),
                      ('/', ' '),
                      ('\\', ' '),
                      ('v p ', 'vp '), ('c e o ', 'ceo '), ('c m o ', 'cmo '),
                      ('c o o ', 'coo '), ('c f o ', 'cfo '), ('c i o ', 'cio ')]
        # Replace Characters in the above list
        for i in ls_replist:
            ls_title = ls_title.replace(i[0], i[1])
        i_title = TitleSearch(ls_title)
        ls_code = ''
        if (i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*ASST.*' '|' r'.*ASSISTANT.*')) or i_title.match(
            r'^AVP .*' '|' r'.* AVP .*' '|' r'.* AVP$' '|' r'^AVP$'):
            ls_code = 'A03'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*' '|'
                           r'.* GM$') and i_title.match(r'.*ASST.*' '|' r'.*ASSIST.*'):
            ls_code = 'B03'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*ASST.*' '|' r'.*ASSISTANT.*'):
            ls_code = 'C03'
        elif i_title.match(r'.*ASST.*' '|' r'.*ASSIST.*') and not i_title.match_t(
                ['ASST', 'ASSISTANT', 'ASSISTANT ', 'ASSISTANT  ']):
            ls_code = '1'
        elif i_title.match_t(['CEO']) or i_title.match(
                r'.*CHIEF.* EXEC.* OFF.*' '|' r'.*CHIEF.* EXEC.* OFF.*' '|' r'^CEO$' '|' 
                r'^CEO .*' '|' r'.* CEO .*' '|' r'^C E O$' '|' r'.* CEO$' '|' r'.*/CEO$' '|' r'.*&CEO$'):
            ls_code = 'ZA'
        elif i_title.match_t(['CMO']) or i_title.match(
                r'.*CHIEF.* MARK.* OFF.*' '|' r'.*CHIEF.* MKTG.* OFF.*' '|' r'^CMO .*' '|'
                r'.* CMO .*' '|' r'^C M O$' '|' r'.* CMO$' '|' r'.*/CMO$' '|' r'.*&CMO$'):
            ls_code = 'ZM'
        elif i_title.match(r'.*CHAIR.*' '|' r'.*CHRM.*' '|' r'^CHM$'):
            ls_code = 'ZB'
        elif i_title.match_t(['COO']) or i_title.match(
                r'.*CHIEF.* OP.* OFF.*' '|' r'.*CHIEF.* OP.* OFF.*' '|' r'^COO .*' '|'
                r'.* COO .*' '|' r'^C O O$' '|' r'.* COO$'  '|' r'.*/COO$' '|' r'.*&COO$'):
            ls_code = 'ZC'
        elif i_title.match_t(['CFO']) or i_title.match(
                r'.*CHIEF.* FIN.* OFF.*' '|' r'.*CHIEF.* FIN.* OFF.*' '|' r'^CFO .*' '|'
                r'.* CFO .*' '|' r'.* CFO$' '|' r'.*/CFO$'  '|' r'.*&CFO$'):
            ls_code = 'ZD'
        elif i_title.match_t(['CTO']) or i_title.match(
                r'.*CHIEF.* TECH.* OFF.*' '|' r'.*CHIEF.* TECH.* OFF.*' '|' r'^CTO .*' '|'
                r'.* CTO .*'):
            ls_code = 'ZF'
        elif i_title.match_t(['CIO']) or i_title.match(
                r'.*CHIEF.* INF.* OFF.*' '|' r'.*CHIEF.* INF.* OFF.*' '|' r'^CIO .*' '|'
                r'.* CIO .*'):
            ls_code = 'ZE'
        elif (i_title.match(r'.*PRESIDENT.*' '|' r'.*PRES .*' '|' r'.*PRES\..*') or
              i_title.match_t(['PRES', 'PREZ', 'PRESIDEN', 'PRESID']) or
              i_title.match(r'^PRES .*' '|' r'.* PRES .*' '|' r'.* PRES$')) and not \
                i_title.match(r'.*VICE.*') and not i_title.match(r'.*VP.*') and not i_title.match(
            r'^V .*') and not i_title.match(r'VICE.*'):
            ls_code = 'ZL'
        elif i_title.match(r'^C(.)O$' '|' r'.* C(.)O$' '|' r'.* C(.)O .*' '|' r'^C(.)O .*' '|'
                           r'.*CHIEF .* OFFICER.*' '|' r'.*CHIEF .* OFF.*'
                           '|' r'^CHIEF .* OFF.*'):
            ls_code = 'Z'
        elif i_title.match(r'.*OWNER.*' '|' r'.*OWNR.*' '|' r'^OWN$' '|' r'^PROP$' '|'
                           r'.*PROPRIETOR.*') or i_title.match_t(['Onwer', '0WNER']):
            ls_code = 'ZG'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(r'^ACTING.*'):
            ls_code = 'A01'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*AREA .*' '|' r'.*UNIT .*' '|' r'.*STAFF .*'):
            ls_code = 'A02'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*ASSOCIATE.*' '|' r'.*ASSOC .*' '|' r'.*ASSC .*'):
            ls_code = 'A04'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*BRANCH.*' '|' r'.*BRNCH.*'):
            ls_code = 'A05'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(r'.*BRAND.*'):
            ls_code = 'A06'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and \
                i_title.match(r'^CORP.*' '|' r'^CRP .*'):
            ls_code = 'A07'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*DEPARTMENT .*' '|' r'.*DEPT .*' '|' r'.*DPT .*'):
            ls_code = 'A08'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(r'.*DEPUTY.*'):
            ls_code = 'A10'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(r'.*DISTRICT.*'):
            ls_code = 'A11'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*DIVISION.*' '|' r'^DIV .*'):
            ls_code = 'A12'
        elif (i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*EXEC.*')) or i_title.match(r'^EVP .*' '|' r'.* EVP .*' '|' r'.* EVP$' '|'
                                               r'^EVP$'):
            ls_code = 'A13'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(r'.*FIELD.*'):
            ls_code = 'A14'
        elif i_title.match(r'^HEAD OF.*'):
            ls_code = 'F15'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*CHIEF.*' '|' r'.*HEAD.*' '|' r'.*FIRST.*' '|' r'.*LEAD.*'):
            ls_code = 'A15'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'^GENERAL.*' '|' r'^GEN .*'):
            ls_code = 'A16'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*GROUP.*' '|' r'.*GRP.*'):
            ls_code = 'A17'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*INTL.*' '|' r'.*INTERNAT.*'):
            ls_code = 'A18'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'^jr .*' '|' r'.*junior.*'):
            ls_code = 'A19'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and (i_title.match(
                r'.*NATIONAL.*') and not i_title.match(r'.*INTER.*')):
            ls_code = 'A20'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*OFFICE .*' '|' r'.*SITE.*'):
            ls_code = 'A21'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(r'.*PLANT .*'):
            ls_code = 'A22'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(r'.*product .*'):
            ls_code = 'A23'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*PROGRAM .*' '|' r'.*PRG .*' '|' r'.*PROG .*'):
            ls_code = 'A24'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*PROJ .*' '|' r'.*PROJECT .*'):
            ls_code = 'A25'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(r'.*PROPERTY.*'):
            ls_code = 'A26'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(r'.*REGION.*'):
            ls_code = 'A27'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*ZONE.*' '|' r'.*SECTION.*'):
            ls_code = 'A28'
        elif (i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and i_title.match(
                r'.*SNR.*' '|' r'^SR .*' '|' r'.*SENIOR.*')) or i_title.match(
            r'^EVP .*' '|' r'.* SVP .*' '|' r'.* SVP$' '|' r'^SVP$' '|' r'^SVP $' '|' r'^SVP  $' 
            '|' r'^SVP .*' ):
            ls_code = 'A29'
        elif i_title.match(r'.*VP.*' '|' r'.*VICE PRES.*') and not i_title.match_t(
                ['VP', 'VP ', 'VP  ', 'VICE PRES', 'VICE PRESIDENT',  'VICE PRESIDENT ',
                 'VICE PRESIDENT  ', 'VICE PRESIDENT   ', 'EX\. V-P\.']):
            ls_code = 'A30'
        elif i_title.match_t(['VP', 'VP ', 'VP  ', 'VICE PRES', 'VICE PRESIDENT', 'VICE PRESIDENT ',
                              'VICE PRESIDENT  ', 'VICE PRESIDENT   ', 'VICEPRESIDENT', 'V P',
                              'V PRESIDENT']) or \
                i_title.match(r'.* V-P\.'):
            ls_code = 'A31'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'^ACTING.*'):
            ls_code = 'C01'
        elif i_title.match(
                r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and i_title.match(
            r'.*AREA .*' '|' r'.*UNIT .*' '|' r'.*STAFF .*'):
            ls_code = 'C02'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*ASSOCIATE.*' '|' r'.*ASSOC .*' '|' r'.*ASSC .*'):
            ls_code = 'C04'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*BRANCH.*' '|' r'.*BRNCH.*'):
            ls_code = 'C05'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*BRAND.*'):
            ls_code = 'C06'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'^CORP.*' '|' r'^CRP .*'):
            ls_code = 'C07'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*DEPARTMENT .*' '|' r'.*DEPT .*' '|' r'.*DPT .*'):
            ls_code = 'C08'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*DEPUTY.*'):
            ls_code = 'C10'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*DISTRICT.*'):
            ls_code = 'C11'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*DIVISION.*' '|' r'^DIV .*'):
            ls_code = 'C12'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*EXEC.*'):
            ls_code = 'C13'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*FIELD.*'):
            ls_code = 'C14'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*CHIEF.*' '|' r'.*HEAD.*' '|' r'.*FIRST.*' '|' r'.*LEAD.*'):
            ls_code = 'C15'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'^GENERAL.*' '|' r'^GEN .*'):
            ls_code = 'C16'
        elif i_title.match(
                r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and i_title.match(
            r'.*GROUP.*' '|' r'.*GRP.*'):
            ls_code = 'C17'
        elif i_title.match(
                r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and i_title.match(
            r'.*INTL.*' '|' r'.*INTERNAT.*'):
            ls_code = 'C18'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'^jr .*' '|' r'.*junior.*'):
            ls_code = 'C19'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                (i_title.match(r'.*NATIONAL.*') and not i_title.match(r'.*INTER.*')):
            ls_code = 'C20'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*OFFICE .*' '|' r'.*SITE.*'):
            ls_code = 'C21'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*PLANT .*'):
            ls_code = 'C22'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*product .*'):
            ls_code = 'C23'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*PROGRAM .*' '|' r'.*PRG .*' '|' r'.*PROG .*'):
            ls_code = 'C24'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*PROJ .*' '|' r'.*PROJECT .*'):
            ls_code = 'C25'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*PROPERTY.*'):
            ls_code = 'C26'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*REGION.*'):
            ls_code = 'C27'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*ZONE.*' '|' r'.*SECTION.*'):
            ls_code = 'C28'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and \
                i_title.match(r'.*SNR.*' '|' r'^SR .*' '|' r'.*SENIOR.*'):
            ls_code = 'C29'
        elif i_title.match(r'.*DIRECTOR.*' '|' r'.* DIR .*' '|' r'^DIR .*' '|' r'.* DIR$') and not \
                i_title.match_t(['DIRECTOR', 'DIRECTOR ', 'DIRECTOR  ', 'DIRECTOR   ', 'DIR']):
            ls_code = 'C30'
        elif i_title.match_t(['DIRECTOR', 'DIRECTOR ', 'DIRECTOR  ', 'DIRECTOR   ','DIR']):
            ls_code = 'C31'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'^ACTING.*'):
            ls_code = 'B01'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*AREA .*' '|' r'.*UNIT .*' '|' r'.*STAFF .*'):
            ls_code = 'B02'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*ASSOCIATE.*' '|' r'.*ASSOC .*' '|' r'.*ASSC .*'):
            ls_code = 'B04'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*BRANCH.*' '|' r'.*BRNCH.*'):
            ls_code = 'B05'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*BRAND.*'):
            ls_code = 'B06'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'^CORP.*' '|' r'^CRP .*'):
            ls_code = 'B07'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*DEPARTMENT .*' '|' r'.*DEPT .*' '|' r'.*DPT .*'):
            ls_code = 'B08'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(
                    r'.*DEPUTY.*'):
            ls_code = 'B10'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*DISTRICT.*'):
            ls_code = 'B11'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*DIVISION.*' '|' r'^DIV .*'):
            ls_code = 'B12'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*EXEC.*'):
            ls_code = 'B13'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*FIELD.*'):
            ls_code = 'B14'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*CHIEF.*' '|' r'.*HEAD.*' '|' r'.*FIRST.*' '|' r'.*LEAD.*'):
            ls_code = 'B15'
        elif (i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$'  '|' r'.*MANAGE.*') and
              i_title.match(r'^GENERAL.*' '|' r'^GEN .*')) or \
                i_title.match(r'.* GM .*' '|' r'^GM .*' '|' r'.* GM$' '|' r'^GM$'):
            ls_code = 'B16'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*GROUP.*' '|' r'.*GRP.*'):
            ls_code = 'B17'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*INTL.*' '|' r'.*INTERNAT.*'):
            ls_code = 'B18'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'^jr .*' '|' r'.*junior.*' '|' r'.*JUNIOR.*'):
            ls_code = 'B19'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and (
                i_title.match(r'.*NATIONAL.*') and not i_title.match(r'.*INTER.*')):
            ls_code = 'B20'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*OFFICE .*' '|' r'^OFF.*' '|' r'.*SITE.*'):
            ls_code = 'B21'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*PLANT .*'):
            ls_code = 'B22'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*product .*' '|' r'.*PRODUCT .*'):
            ls_code = 'B23'
        elif (i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(
                    r'.*PROGRAM .*' '|' r'.*PRG .*' '|' r'.*PROG .*' '|' r'^PROGRAM MNGR$')) or \
                i_title.match(r'^PROGRAM MNGR$'):
            ls_code = 'B24'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*PROJ .*' '|' r'.*PROJECT .*'):
            ls_code = 'B25'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*PROPERTY.*'):
            ls_code = 'B26'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*REGION.*'):
            ls_code = 'B27'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*ZONE.*' '|' r'.*SECTION.*'):
            ls_code = 'B28'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and \
                i_title.match(r'.*SNR.*' '|' r'^SR .*' '|' r'.*SENIOR.*'):
            ls_code = 'B29'
        elif i_title.match(r'.*MGR.*' '|' r'.*MGMT.*' '|' r'.* MAN$' '|' r'.*MANAGE.*') and not \
                i_title.match_t(['MGR', 'MANAGER', 'MANAGER ', 'MANAGER  ', 'MANGEMENT', 'MGMT',
                                 'MANAGEMENT', 'MANAGEMENT ']):
            ls_code = 'B30'
        elif i_title.match_t(
                ['MGR', 'MANAGER', 'MANGEMENT', 'MGMT', 'MANAGEMENT', 'MANAGEMENT ', 'MANAGER ',
                  'MANAGER  ','MANGER ', 'MANGER', 'MNGR']):
            ls_code = 'B31'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'^ACTING.*'):
            ls_code = 'D01'
        elif i_title.match(r'.*COOR.*') and i_title.match(
                r'.*AREA .*' '|' r'.*UNIT .*' '|' r'.*STAFF .*'):
            ls_code = 'D02'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*ASST.*' '|' r'.*ASSISTANT.*'):
            ls_code = 'D03'
        elif i_title.match(r'.*COOR.*') and i_title.match(
                r'.*ASSOCIATE.*' '|' r'.*ASSOC .*' '|' r'.*ASSC .*'):
            ls_code = 'D04'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*BRANCH.*' '|' r'.*BRNCH.*'):
            ls_code = 'D05'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*BRAND.*'):
            ls_code = 'D06'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'^CORP.*' '|' r'^CRP .*'):
            ls_code = 'D07'
        elif i_title.match(r'.*COOR.*') and i_title.match(
                r'.*DEPARTMENT .*' '|' r'.*DEPT .*' '|' r'.*DPT .*'):
            ls_code = 'D08'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*DEPUTY.*'):
            ls_code = 'D10'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*DISTRICT.*'):
            ls_code = 'D11'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*DIVISION.*' '|' r'^DIV .*'):
            ls_code = 'D12'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*EXEC.*'):
            ls_code = 'D13'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*FIELD.*'):
            ls_code = 'D14'
        elif i_title.match(r'.*COOR.*') and i_title.match(
                r'.*CHIEF.*' '|' r'.*HEAD.*' '|' r'.*FIRST.*' '|' r'.*LEAD.*'):
            ls_code = 'D15'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'^GENERAL.*' '|' r'^GEN .*'):
            ls_code = 'D16'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*GROUP.*' '|' r'.*GRP.*'):
            ls_code = 'D17'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*INTL.*' '|' r'.*INTERNAT.*'):
            ls_code = 'D18'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'^jr .*' '|' r'.*junior.*'):
            ls_code = 'D19'
        elif i_title.match(r'.*COOR.*') and (i_title.match(r'.*NATIONAL.*') and not
        i_title.match(r'.*INTER.*')):
            ls_code = 'D20'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*OFFICE .*' '|' r'.*SITE.*'):
            ls_code = 'D21'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*PLANT .*'):
            ls_code = 'D22'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*product .*'):
            ls_code = 'D23'
        elif i_title.match(r'.*COOR.*') and i_title.match(
                r'.*PROGRAM .*' '|' r'.*PRG .*' '|' r'.*PROG .*'):
            ls_code = 'D24'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*PROJ .*' '|' r'.*PROJECT .*'):
            ls_code = 'D25'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*PROPERTY.*'):
            ls_code = 'D26'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*REGION.*'):
            ls_code = 'D27'
        elif i_title.match(r'.*COOR.*') and i_title.match(r'.*ZONE.*' '|' r'.*SECTION.*'):
            ls_code = 'D28'
        elif i_title.match(r'.*COOR.*') and i_title.match(
                r'.*SNR.*' '|' r'^SR .*' '|' r'.*SENIOR.*'):
            ls_code = 'D29'
        elif i_title.match(r'.*COOR.*') and not i_title.match_t(['COOR', 'COORD', 'COORDINATOR',
                                                                 'COORDINATOR ', 'COORDINATOR  ']):
            ls_code = 'D30'
        elif i_title.match_t(['COOR', 'COORD', 'COORDINATOR', 'COORDINATOR ', 'COORDINATOR  ']):
            ls_code = 'D31'
        elif i_title.match(r'.*SUPERINTENDENT.*') and not i_title.match_t(['SUPERINTENDENT',
                                                       'SUPERINTENDENT ', 'SUPERINTENDENT  ']):
            ls_code = 'E'
        elif i_title.match_t(['SUPERINTENDENT', 'SUPERINTENDENT ', 'SUPERINTENDENT  ']):
            ls_code = 'E31'
        elif i_title.match(r'.*SNR.*' '|' r'^SR .*' '|' r'.*SENIOR.*'):
            ls_code = 'F'
        elif i_title.match(r'.*DEPARTMENT .*' '|' r'.*DEPT .*' '|' r'.*DPT .*'):
            ls_code = 'G'
        elif i_title.match(r'.*FIELD.*'):
            ls_code = 'H'
        elif i_title.match(r'^jr .*' '|' r'.*junior.*' '|' r'.*JUNIOR.*'):
            ls_code = 'I'
        elif i_title.match(r'.*DEPUTY.*') and not i_title.match_t(['DEPUTY']):
            ls_code = 'J'
        elif i_title.match_t(['DEPUTY']):
            ls_code = 'J31'
        elif i_title.match(r'^CONTRACTING') or i_title.match(r'.*CONTRACTING.*'):
            ls_code = ''
        elif i_title.match(r'.*ACTING.*') and not i_title.match(r'.*COACH.*'):
            ls_code = 'K'
        elif i_title.match(r'.*AREA.*'):
            ls_code = 'L'
        elif i_title.match(r'.*STAFF.*') and not i_title.match_t(['STAFF']):
            ls_code = 'M'
        elif i_title.match_t(['STAFF']):
            ls_code = 'M31'
        elif i_title.match(r'.*ASSOCIATE.*' '|' r'.*ASSOC .*' '|' r'.*ASSC .*') and \
                not i_title.match_t(['ASSOCIATE', 'ASSOCIATE ', 'ASSOCIATE  ', 'ASSOC', 'ASSC']):
            ls_code = 'N'
        elif i_title.match_t(['ASSOCIATE', 'ASSOCIATE ', 'ASSOCIATE  ', 'ASSOC', 'ASSC']):
            ls_code = 'N31'
        elif i_title.match(r'.*GROUP.*' '|' r'.*GRP.*'):
            ls_code = 'O'
        elif i_title.match(r'.*DIVISION.*' '|' r'^DIV .*'):
            ls_code = 'P'
        elif i_title.match(r'.*NATL.*' '|' r'.*NATIONAL.*'):
            ls_code = 'Q'
        elif i_title.match(r'.*INTL.*' '|' r'.*INTERNAT.*'):
            ls_code = 'R'
        elif i_title.match(r'.*BRANCH.*'):
            ls_code = 'S'
        elif i_title.match(r'.*PROJ .*' '|' r'.*PROJECT .*') or i_title.match_t(
                ['PROJECT', 'PROJECTS']):
            ls_code = 'T'
        elif i_title.match(r'.*REGION.*'):
            ls_code = 'U'
        elif i_title.match_t(['ASST', 'ASSISTANT', 'ASSISTANT ', 'ASSISTANT  ']):
            ls_code = '131'
        elif i_title.match(r'.*EXEC .*' '|' r'.*EXECUTIVE.*' '|' r'^EXE  .*' '|' r'.* EXEC$' '|'
                           r'.* EXECUTIVE$') and not i_title.match_t(['EXEC', 'EXE', 'EXECUTIVE']):
            ls_code = 'V'
        elif i_title.match_t(['EXEC', 'EXE', 'EXECUTIVE']):
            ls_code = 'V31'
        elif i_title.match_t(['OFFICER', 'OFFICER ' ]):
            ls_code = 'X31'
        elif i_title.match_t(['CHIEF']):
            ls_code = 'Y31'
        elif i_title.match(r'.* REP$' '|' r'.* REP $' '|' r'.* REP  $' '|' r'.*REPRESENTAT.*') and\
                not i_title.match_t(['REP', 'REP ', 'REPRESENTATIVE', 'REPRESENTATIVE ']):
            ls_code = '2'
        elif i_title.match_t(['REP', 'REP ', 'REP  ', 'REPRESENTATIVE', 'REPRESENTATIVE ']):
            ls_code = '231'
        elif i_title.match(r'.*CHIEF .*OFF.*' '|' r'^CHIEF .*OFF.*'):
            ls_code = 'Z'
        elif i_title.match(r'.*OFFICER.*' '|' r'.* OFF .*') and not i_title.match_t(['OFFICER']):
            ls_code = 'X'
        elif (i_title.match(r'.*PARTNER.*') and not i_title.match(r'.*ship.*')) or i_title.match_t(
                ('PTR', 'PRTNR', 'PTNR.', 'PTNR')) or i_title.match(r'.*PRTNR.*'):
            ls_code = 'ZH'
        elif i_title.match(r'.*PRINCIPAL.*' '|' r'^PRIN$' '|' r'^PRINC$'):
            ls_code = 'ZI'
        elif i_title.match(r'.*EXEC.* ADM.*'):
            ls_code = 'ZJ'
        elif i_title.match(r'.*ADMIN.*') and not i_title.match_t(
                ['ADMIN', 'ADMINISTRATION', 'ADMINISTRATOR', 'ADMIN ',
                 'ADMINISTRATION ', 'ADMINISTRATOR ', 'ADMINISTRATOR  ']):
            ls_code = 'W'
        elif i_title.match_t(['ADMIN', 'ADMINISTRATION', 'ADMINISTRATOR', 'ADMIN ',
                              'ADMINISTRATION ', 'ADMINISTRATOR ', 'ADMINISTRATOR  ']):
            ls_code = 'W31'
        elif i_title.match(r'.*SUPERV.*' '|' r'.*SUPV.*'):
            ls_code = 'W2'
        elif i_title.match(r'.*CHIEF.*') and not i_title.match_t(['CHIEF']):
            ls_code = 'Y'
        elif i_title.match_t(['Board Member', 'Board Member ', 'Board Member  ', 'Board Member   ',
                              'OTHER BOARD MEMBERS']):
            ls_code = 'X01'
        elif i_title.match_t(['Member', 'Member ',  'Member  ','MEMBERS']):
            ls_code = 'X02'
        elif i_title.match_t(['BOARD OF TRUSTEES MEMBER', 'BOARD OF TRUSTEES']):
            ls_code = 'X03'
        elif i_title.match(r'^Council Member$' '|' r'^Council Member $' '|' r'^Council Member  $'):
            ls_code = 'X04'
        elif i_title.match(r'^Managing Member$'):
            ls_code = 'X05'
        elif i_title.match(r'^BOARD MEMBERS$'):
            ls_code = 'X06'
        elif i_title.match(r'^COMMITTEE MEMBER$'):
            ls_code = 'X07'
        elif i_title.match(r'^FOUNDING MEMBER$'):
            ls_code = 'X08'
        elif i_title.match_t(['FOUNDER', 'FOUNDER ', 'CO-FOUNDER', 'CO FOUNDER ', 'CO FOUNDER  ',
                              'CO FOUNDER', 'Co Founder', 'FOUNDERS']):
            ls_code = 'X09'
        elif i_title.match_t(['PROGRAM', 'PROGRAMS']):
            ls_code = 'E24'
        return ls_code
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


# Execute `main()` function
if __name__ == '__main__':
    # print(get_title_code(' SVP  MORTGAGE  BUS. DEV.  STRATEGIC ALLIANCES'))
    # print(get_title_code('   ASST CMO'))
    # print(get_title_code('CHIEF EXEC OFF'))
    print(get_title_code('ENGINEERING AIDE III / SUBCONTRACT TECHNICAL REP '))
