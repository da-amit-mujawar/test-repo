create or replace function udf_sim_enhanced_matchcode2(FirstName varchar(max), LastName varchar(max), Address varchar(max), Zip varchar(max), Company varchar(max))
returns varchar(max)
stable
as $$

    def format_match_string(str_or_none):
        if str_or_none is None:
            return "NULL"
        else:
            return str_or_none.upper().strip()

    def null_to_empty(s):
            if s is None:
                return "NULL"
            return str(s.strip())

    def replacechars(inputstr, replacechars, replacewith):
        if inputstr is None:
            return ""
        for c in replacechars:
            inputstr = inputstr.replace(c, replacewith)
        return inputstr.strip()

    def get_mc_street(straddress, intmaxlen):
        if straddress is None:
            return "NULL"
        mc_street = ''
        try:
            i = 0
            while (i < len(straddress)) and (i < intmaxlen):
                if straddress[i].isdigit():
                    mc_street = mc_street + straddress[i]
                else:
                    break
                i += 1
        except:
            pass
        return mc_street.strip()

    def get_mcstring_address(straddress):
        if straddress is None:
            return 'NULL'

        strvowels = ''
        strnumbers = ''
        strconsonants = ''
        straddress = replacechars(straddress, '-.,#', '').upper()
        if straddress != '' and straddress[:1].isdigit():
            mc_street = get_mc_street(straddress, 4)
            len_nextspace = len(mc_street)
            while len_nextspace < len(straddress):
                if straddress[len_nextspace] == ' ':
                    break
                len_nextspace += 1
            mc_street = mc_street.ljust(4, '*')

            while len_nextspace < (len(straddress)):
                asciiValue = ord(straddress[len_nextspace])
                if not asciiValue in range(65, 90 + 1):
                    len_nextspace += 1
                    continue
                if len(mc_street) == 4:
                    mc_street = mc_street + straddress[len_nextspace]
                    len_nextspace += 1
                    continue
                if straddress[len_nextspace] in 'A,a,E,e,I,i,O,o,U,u':
                    strvowels = strvowels + straddress[len_nextspace]
                else:
                    if len(mc_street) in (4, 5):
                        mc_street = mc_street + straddress[len_nextspace]
                if len(mc_street) == 6:
                    break
                len_nextspace += 1

            if len(mc_street) < 6:
                mc_street = mc_street + strvowels[: 6 - len(mc_street)]
        else:
            mc_street = straddress[:1]
            li = len(mc_street)
            while li < (len(straddress)):
                asciiValue = ord(straddress[li])
                if (asciiValue not in range(65, 90 + 1) and straddress[li].isdigit() == False):
                    li += 1
                    continue
                if straddress[li].isdigit():
                    strnumbers = strnumbers + straddress[li]
                else:
                    if straddress[li] in 'A,a,E,e,I,i,O,o,U,u':
                        strvowels = strvowels + straddress[li]
                    else:
                        strconsonants = strconsonants + straddress[li]

                if len(strnumbers) >= 4 and len(strconsonants) >= 2:
                    break

                li += 1
            mc_street = strnumbers[:4]
            mc_street = mc_street.ljust(4, '*')
            if len(straddress)>0:
                mc_street = mc_street + straddress[0]
            mc_street = mc_street + strconsonants[:1]

            if len(mc_street) < 6:
                mc_street = (mc_street + strvowels)[:6]
        mc_street = mc_street.ljust(6, '*')
        return mc_street



    def get_mcstring_names(strname, ilen):
        if strname is None:
            return 'NULL'
        strname = replacechars(strname, ".', ", '').upper()
        mc_strname = strname[:1]
        strvowels = ''
        li = 1
        while li < len(strname) and len(mc_strname) < ilen:
            if strname[li] in 'A,a,E,e,I,i,O,o,U,u':
                strvowels = strvowels + strname[li]
            else:
                mc_strname = mc_strname + strname[li]
            li += 1
        if len(mc_strname) < ilen:
            mc_strname = mc_strname + strvowels[: ilen - len(mc_strname)]

        mc_strname = mc_strname.ljust(ilen, '*')

        mc_strname = mc_strname.ljust(ilen, '*')
        return mc_strname

    def get_mcstring_company(strname, ilen):
        if strname is None:
            return 'NULL'

        strname = replacechars(strname, ".',&", '').upper().strip()
        strname = strname.replace('  ', ' ')
        lcword1 = ''
        lcword2 = ''
        splitlist = strname.split()
        if len(splitlist) > 1:
            lcword1 = splitlist[0].strip()
            lcword2 = splitlist[1].strip()
        elif len(splitlist) == 1:
            lcword1 = splitlist[0].strip()

        if lcword1 == '':
            lcword1 = lcword1[:ilen]
        else:
            lcword1 = lcword1[:2].strip().ljust(2, '*')
            lcword2 = lcword2[:2].strip().ljust(2, '*')

        return (lcword1 + lcword2).ljust(ilen, '*')

    def get_mcstring_zip(zip, ilen):
        if zip is None:
            return 'NULL'
        return zip.strip().ljust(ilen, '*')



    matchmode = ''
    mc_address = ''
    mc_lastname = ''
    mc_firstname =''
    mc_zip =''
    try:
        mc_address = get_mcstring_address(Address)
        mc_lastname  = get_mcstring_names (LastName,3)
        mc_firstname = get_mcstring_names(FirstName,3)
        mc_company  = get_mcstring_company (Company,4)
        mc_zip = get_mcstring_zip(Zip, 5)
        if mc_lastname == 'NULL' or mc_firstname == 'NULL' or mc_zip == 'NULL'  or mc_address == 'NULL' or mc_company == 'NULL':
            matchmode = 'NULL'
        else:
            matchmode = mc_zip[:5] + mc_address[:6] + mc_company[:4] + mc_lastname[:3] + mc_firstname[:3]
    except:
        print(sys.exc_info())
    return matchmode
$$ language plpythonu;
