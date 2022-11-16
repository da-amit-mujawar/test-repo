create or replace function LastClickOpenEmailFlagUpdate(
intGlobalIDMSIDParameter int default 0,
strGlobalLogFile varchar(max),
MyLogFile StreamWriter

--returns varchar(max)
--stable
   -- as $$

    --this console application loops through all the Edith roman lists in IDMS and updates Last Click Open date
    --Logic:
    --Provider loads the engagement date file in FTP3.databasedirect.com  Id is idmsedata,
    --the file will end up in \\neptune.ddi.hq.edithroman.com\S-Drive\FTP\usr\idmsedata

    -- We move the file from FTP to local location   \\neptune.ddi.hq.edithroman.com\S-Drive\IDMSFILES\EngageData
    -- we will keep the filename constant.

    --First Parameter: IMDSID.  if passed, execute only for that IDMS DbID.  IF Zero or blank,  Pull ALL

    --command line
    --   CD C:\Development\VB\VS2010 Projects\LastClickOpenEmailFlagUpdate\bin\Debug
    --   LastClickOpenEmailFlag 123343
    --Select top 2000 * from common.dbo.IDMS_BackendUpdateLog  where BackEndProcess ='EngagementData' Order by ID DESC

    --Reju Mathew 2013.09.13


    Sub Main(ByVal sArgs() As String)
        Un_global.GetConfigurationValues()

        --For Testing .. 'Un_global.SendEmailNotification("")

        If Not ReadParametersAndSetVariables(sArgs) Then
            Return
        End If

        Try
            If LoadEngagementDataFile() Then
                ReadePostToIdmsListAndProcess()
            End If


            --Remove the table created for updated.
            --    Un_global.DropIQtable(Un_global.ODBC_IQProd2, Un_global.strTempEngageDataTableName_dist)
        Catch ex As Exception
            SendErrorEmail("Error in processing Engagement Data ( Last Click /Open)" & ex.Message)
        End Try


    End Sub

    def ReadParametersAndSetVariables(sArgs() As String) As Boolean
        Try
            -- find the Paramertes now.

            intGlobalIDMSIDParameter = 0

            If ((sArgs.Length = 0) Or (sArgs(0) = "-h") Or (sArgs(0) = "-H")) Then
                Console.WriteLine("First Parmeter. IMDSID.  if passed, exeute only for that List.  IF Zero or blank,  Pull ALL")
                Return False 'exit  from here
            End If

            Dim strParameter As String = ""
            Dim i As Integer = 0
            While i < sArgs.Length
                strParameter = sArgs(i)
                If i = 0 Then
                    intGlobalIDMSIDParameter = Val(strParameter)
                End If
                i = i + 1                       'Increment to the next argument
            End While


            --Console.WriteLine("Start Exporting files from HH and OO Tables and Update IQ table...")

            If intGlobalIDMSIDParameter = 0 Then             --If there are no arguments
                Console.WriteLine("Updating Last Click  Open  for IDMSID:: ALL")
            Else
                Console.WriteLine("Updating Last Click  Open  for IDMSID: " + intGlobalIDMSIDParameter.ToString)
            End If

            ' Console.ReadLine()  'to pause the cusrsor
            Return True

        Catch ex As Exception
            Console.WriteLine("Error in parameters")
            Console.WriteLine("First Parmeter. IMDSID.  if passed, exeute only for that List.  IF Zero or blank,  Pull ALL")
            Return False
        Finally
        End Try
    return True


    def LoadEngagementDataFile() As Boolean
        Dim strSQL As String = ""
        Dim strReturnResult As String = ""
        Dim StrFTPFileName As String = ""
        Dim strDesitnationFullFileName As String = ""
        Dim strRecordsLoaded As String = "0"
        Dim stTempIQTableName As String = Un_global.strTempEngageDataTableName
        Dim stTempIQTableName_Dist As String = Un_global.strTempEngageDataTableName_dist

        --Input file name
        StrFTPFileName = Un_global.EngageDataFTPFolder.Trim + "\" + Un_global.EngageDataFileName.Trim
        strDesitnationFullFileName = Un_global.strEngageDataProcessFolder + "\" + Un_global.EngageDataFileName.Trim

        --Let us move the file from FTP source folder to internal  folder.
        If Not Un_global.MoveFile(StrFTPFileName, strDesitnationFullFileName) Then
            SendErrorEmail("Error in Engagement load file: Unable to move File  from " & StrFTPFileName + " To " + strDesitnationFullFileName)
            Return False
        End If


        strDesitnationFullFileName = strDesitnationFullFileName.Replace("\", "\\")

        --input IQ Table name,  we use a Temp Table name

        Try

            Un_global.DropIQtable(Un_global.ODBC_IQProd2, stTempIQTableName)
            Un_global.DropIQtable(Un_global.ODBC_IQProd2, stTempIQTableName_Dist)

            strSQL = GetSQLForLoadLastClickOpen(stTempIQTableName, stTempIQTableName_Dist, strDesitnationFullFileName)

            -- MyLogFile.WriteLine(strSQL)
            Un_global.ExecuteNonQueryAndGetResult(Un_global.ODBC_IQProd2, strSQL, strRecordsLoaded)


            Try
                If Convert.ToInt32(strRecordsLoaded) < 1 Then strRecordsLoaded = "0"
            Catch
            End Try
            Un_global.EngageDataNumberofRecords = strRecordsLoaded

            Console.WriteLine("Engagement Records Loaded: " + Un_global.EngageDataNumberofRecords.ToString.Trim)

            --Remove the Gross table now.
            Un_global.DropIQtable(Un_global.ODBC_IQProd2, stTempIQTableName)

            Return True
        Catch ex As Exception
            Console.WriteLine("Error in Engagement load file:" & ex.Message)
            SendErrorEmail("Error in Engagement load file:" & strSQL + vbCrLf & ex.Message)
            Return False
        Finally
        End Try
    End Function

    Private def GetSQLForLoadLastClickOpen(strTableName As String, strTableName_dist As String, strfullfileName As String) As String
        Dim strIDMSFileLocation As String = strfullfileName.Trim
        strTableName = strTableName.Trim.ToUpper

        Dim StrSampleLoad As String = ""
        Dim StrUpdateString As String = ""


        Dim strResult As String = " " + vbCrLf + _
                   "IF EXISTS(SELECT 1 FROM sysobjects WHERE UPPER(name) = UPPER('" + strTableName + "') AND type = 'U') THEN  DROP TABLE " + strTableName + "; END IF;" + vbCrLf + _
                   " CREATE TABLE " + strTableName + " (EmailAddress varchar(200), OpenDate VARCHAR(20), ClickDate VARCHAR(20), Filler_Opened VARCHAR(20), Filler_Clicked VARCHAR(20), Filler_Optout VARCHAR(20), OpenclickDate VARCHAR(6) );" + vbCrLf + _
                   " Commit;" + vbCrLf + _
                   " " + vbCrLf + _
                   " LOAD TABLE " + strTableName + "  " + vbCrLf + _
                   " ( " + vbCrLf + _
                   " EmailAddress ',', " + vbCrLf + _
                   " OpenDate ',', " + vbCrLf + _
                   " ClickDate ',', " + vbCrLf + _
                   " Filler_Opened ',', " + vbCrLf + _
                   " Filler_Clicked ',', " + vbCrLf + _
                   " Filler_Optout '\x0d\x0a' " + vbCrLf + _
                   " ) " + vbCrLf + _
                   " FROM '" + strIDMSFileLocation + "' " + vbCrLf + _
                   " format ascii " + vbCrLf + _
                   " quotes off " + vbCrLf + _
                   " escapes off " + vbCrLf + _
                   StrSampleLoad + vbCrLf + _
                   " ;  " + vbCrLf + _
                   " commit;  " + vbCrLf + _
                   "  " + vbCrLf + _
                   " --Delete the header " + vbCrLf + _
                   " DELETE from " + strTableName + "  where LTRIM(RTRIM(UPPER(EmailAddress))) ='EMAILADDRESS'   or EmailAddress ='' OR  LTRIM(RTRIM(UPPER(EmailAddress))) ='EMAIL'; " + vbCrLf + _
                   " commit; " + vbCrLf + _
                   "  " + vbCrLf + _
                   "  " + vbCrLf + _
                   " Update " + strTableName + "  Set OpenclickDate = LEFT(OpenDate,6) From " + strTableName + " A  where OpenDate <>''; " + vbCrLf + _
                   " Commit; " + vbCrLf + _
                   "  " + vbCrLf + _
                   " Update " + strTableName + " Set OpenclickDate = LEFT(Clickdate,6) From " + strTableName + " A where OpenDate ='' and  OpenclickDate is null and Clickdate <>''; " + vbCrLf + _
                   " Commit; " + vbCrLf + _
                   "  " + vbCrLf + _
                   "  " + vbCrLf + _
                   " CREATE TABLE " + strTableName_dist + " (EmailAddress varchar(65), OpenClickDate VARCHAR(6));" + vbCrLf + _
                   " INSERT INTO " + strTableName_dist + "(EmailAddress, OpenClickDate) " + vbCrLf + _
                   " Select UPPER(LEFT(LTRIM(RTRIM(EmailAddress)), 65)) as EmailAddress , MAX(OpenClickDate) as OpenClickDate " + vbCrLf + _
                   " from " + strTableName + " " + vbCrLf + _
                   " group by EMAILADDRESS " + vbCrLf + _
                   " order by EMAILADDRESS; " + vbCrLf + _
                   " commit; " + vbCrLf + _
                   "  " + vbCrLf + _
                  "  " + vbCrLf

        Return strResult
    End def

    def ReadePostToIdmsListAndProcess() As Boolean
        Dim strwhere As String = ""  -- Set IDMS id later
        Dim strSQL As String = ""
        Dim intIDMSID As Integer = 0
        Dim strIQTableName As String = ""


        Try

            --strGlobalLogFile = Un_global.ExportFolder + "\IDMSEngagementDataLogFile.txt"
            strGlobalLogFile = "IDMSEngagementDataLogFile.txt"
            MyLogFile = New StreamWriter(strGlobalLogFile)

            If intGlobalIDMSIDParameter > 0 Then
                strwhere = " AND IDMSID =" + intGlobalIDMSIDParameter.ToString.Trim
            End If
            Dim myConnection As Odbc.OdbcConnection = Un_global.GetConnectionObject(Un_global.ODBC_DWSQLPRD4)
            myConnection.Open()
            strSQL = "SELECT Distinct  IDMSID  from Common.[dbo].[ePostToIdmsList]  where boolActive = 1 And IDMSID > 0  and ApplyEngageData =1 " + strwhere + " order by 1"
            MyLogFile.WriteLine(strSQL)
            Dim myReader As Odbc.OdbcDataReader = Un_global.CreateAndExecuteSqlDataReader(myConnection, strSQL)
            -- Console.WriteLine(strSQL)
            While myReader.Read()  -- we expect oly one record at a time

                intIDMSID = myReader.GetValue(0)
                If intIDMSID Then
                    Console.WriteLine("Updating EngagementData for " + intIDMSID.ToString + "...")
                    UpdateIQTable(intIDMSID)
                End If
            End While

            SendEmailwithFinalReport("")

            myConnection.Close()
            myReader.Close()
            MyLogFile.Close()
            Return True
        Catch ex As Exception
            Console.WriteLine("Error in ReadePostToIdmsListAndProcess:" & ex.Message)
            SendErrorEmail("Error in LastClickOpenFlag - ReadePostToIdmsListAndProcess" & strSQL + vbCrLf & ex.Message)
            Return False
        Finally

        End Try
    End Function


    Function UpdateIQTable(intIDMSID As Integer) As Boolean
        Dim strSQL As String = ""
        Dim strReturnResult As String = ""
        Dim strRecordsLoaded As String = "0"
        Dim strRecordsUpdated As String = "0"
        Dim strIQMainTableName = GetIQTableName(intIDMSID)

        If strIQMainTableName <> "" Then
            Try

                CreateFieldIfnotFound(strIQMainTableName)
                strSQL = "" + vbCrLf
                strSQL = strSQL + "--Set LastClickOpen ..." + vbCrLf
                strSQL = strSQL + "UPDATE " + strIQMainTableName.Trim + vbCrLf
                strSQL = strSQL + "  SET LastClickOpen  = b.OpenClickDate  " + vbCrLf
                strSQL = strSQL + "FROM " + strIQMainTableName.Trim + " A" + vbCrLf
                strSQL = strSQL + "INNER JOIN " + Un_global.strTempEngageDataTableName_dist + " B ON A.EmailAddress = B.EmailAddress " + vbCrLf
                strSQL = strSQL + "WHERE (LastClickOpen IS NULL OR LastClickOpen ='') OR (LastClickOpen  <> b.OpenClickDate) ;"
                strSQL = strSQL + " commit; " + vbCrLf

                MyLogFile.WriteLine(strSQL)

                If My.Settings.DebugFlag.ToUpper <> "TRUE" Then
                    Un_global.ExecuteNonQueryAndGetResult(Un_global.ODBC_IQProd2, strSQL, strRecordsUpdated)
                End If

                Try
                    If Convert.ToInt32(strRecordsUpdated) < 1 Then strRecordsUpdated = "0"
                Catch
                End Try

                Console.WriteLine(" Records Updated: " + strRecordsUpdated)

                If strRecordsUpdated <> "0" Then
                    UpdateLogTable(intIDMSID, strRecordsUpdated, strIQMainTableName)
                End If


            Catch ex As Exception
                Console.WriteLine("Error in UpdateIQTable:" & ex.Message)
                SendErrorEmail("Error in UpdateIQTable:" & strSQL + vbCrLf & ex.Message)
                Return False
            Finally
            End Try
        End If
        Return True
    End Function

    Function CreateFieldIfnotFound(strIQMainTableName As String) As Boolean
        Dim strSQL As String = ""
        Dim strReturnResult As String = ""
        Dim strRecordsUpdated As Integer = 0

        If strIQMainTableName <> "" Then
            If Not Un_global.isFieldFoundinIQtable(Un_global.ODBC_IQProd2, strIQMainTableName, "LastClickOpen") Then
                Try
                    strSQL = "ALTER TABLE " + strIQMainTableName.Trim + " ADD LastClickOpen  varchar(6); " + vbCrLf
                    If Un_global.ExecuteNonQueryAndGetResult(Un_global.ODBC_IQProd2, strSQL, strRecordsUpdated) Then
                        strSQL = "CREATE LF INDEX LF_" + strIQMainTableName.Trim + "_LastClickOpen ON " + strIQMainTableName.Trim + " (LastClickOpen)  in ""iq_indexes""; " + vbCrLf
                        Un_global.ExecuteNonQueryAndGetResult(Un_global.ODBC_IQProd2, strSQL, strRecordsUpdated)
                    End If

                Catch
                Finally
                End Try
            End If
        End If
        Return True
    End Function


    Function UpdateLogTable(intIDMSID As Integer, strRecordsUpdated As String, strIQMainTableName As String) As Boolean
        Dim strSQL As String = ""
        Dim strReturnResult As String = ""
        Dim StrType = ""
        Dim StrBackEndProcess As String = "EngagementData"
        Dim intQtyReceived As Integer = Un_global.EngageDataNumberofRecords

        Try
            strSQL = "INSERT INTO common.dbo.IDMS_BackendUpdateLog (IDMSID,BackEndProcess, cType, QtyReceived,QtyApplied, IQMainTableName) " + _
                    " Values (" + intIDMSID.ToString.Trim + ",'" + StrBackEndProcess + "','" + StrType + "'," + intQtyReceived.ToString.Trim + "," + strRecordsUpdated + ",'" + Left(strIQMainTableName.Trim, 30) + "')"
            MyLogFile.WriteLine(strSQL)
            If Un_global.ExecuteNonQueryAndGetResult(Un_global.ODBC_DWSQLPRD4, strSQL, strReturnResult) Then
                Return True
            Else
                Console.WriteLine("Error in UpdateLogFile:")
            End If
            Return True
        Catch ex As Exception
            Console.WriteLine("Error in UpdateLogFile:" & ex.Message)
            SendErrorEmail("Error in UpdateLogFile:" & ex.Message)
            Return False
        Finally
        End Try
    End Function

    Function GetIQTableName(intIDMSID As Integer) As String
        Dim strSQL As String = ""
        Try
            Dim strResult As String = ""
            strSQL = "SELECT cTableName FROM DW_Admin.dbo.tblBuildTable WHERE cTableName like 'tblMain_%' AND BuildID IN " + _
                   " (SELECT TOP 1 ID FROM DW_Admin.dbo.tblBuild WHERE iIsOnDisk = 1 AND DatabaseID = " + intIDMSID.ToString.Trim + " ORDER BY ID DESC)"
            MyLogFile.WriteLine(strSQL)
            strResult = Un_global.GetSingleFieldFromSQL(Un_global.ODBC_DWSQLPRD1, strSQL)
            strResult = strResult.ToUpper.Trim

            If My.Settings.DebugFlag.ToUpper = "TRUE" Then
                --For Testing.  Remove these 2 lines
                Console.Write("Testing on tblMain_1249_201212_RejuDelete....")
                strResult = "tblMain_1249_201212_RejuDelete"
            End If

            If strResult = "" Then
                SendErrorEmail(vbCrLf + vbCrLf + "Error: No IDMS Build found for IDMSID:" & intIDMSID.ToString + vbCrLf + vbCrLf + strSQL)
            End If

            MyLogFile.WriteLine("IQTable Used for Update: " + strResult)


            Return strResult
        Catch ex As Exception
            Console.Write("Can't connect SQl Server or Error in SQL.  Please review SQL." + strSQL + ex.Message)
            SendErrorEmail("Can't connect SQl Server or Error in SQL.  Please review SQL." + strSQL + ex.Message)
            Return ""
        Finally
        End Try

    End Function


    Function SendEmailwithFinalReport(ByRef strResultMessage As String) As Boolean
        -- Reju Mathew 2010.12.15
        -- Read the tblReplyExportHistory  table and send emails as attachment.

        Dim strSendEmailTo As String = My.Settings.SendEmailTo
        Dim strFileName As String = ""
        Dim StrMessage As String = ""

        --email variables
        Dim strSendFrom As String = My.Settings.NotificationEmailSendFrom
        Dim strDefaultSubjectLine As String = My.Settings.DefaultSubjectLine
        Dim strDefaultMessageBody As String = My.Settings.DefaultMessage
        Dim strReportFileName As String = Un_global.ExportFolder + "\IDMSEngagementDataupdateReport.txt"

        Try
            Dim strSQL As String = ""
            strSQL = "SELECT TOP 200  IDMSID, IQMainTableName, cType, QtyReceived, QtyApplied, dRecordDate  " + _
                    " from common.dbo.IDMS_BackendUpdateLog  where BackEndProcess ='EngagementData'" + _
                    " Order by ID DESC  "

            MyLogFile.WriteLine(strSQL)
            Un_global.WriteTheOutPutFile_Report(strReportFileName, strSQL)


            StrMessage = strDefaultMessageBody + vbCrLf + "Engagement data is applied on IDMS tables.  See attached file for top 200 log records." + vbCrLf
            StrMessage = StrMessage + vbCrLf + "You can find the log table under  DWSQLPROD4. " + vbCrLf + " Select top 200 * from common.dbo.IDMS_BackendUpdateLog  where BackEndProcess ='EngagementData' Order by ID DESC  "
            If Un_global.SendEMailUsingSMTP(strSendEmailTo, strSendFrom, strDefaultSubjectLine, StrMessage, strReportFileName, strResultMessage) Then
                Console.Write("Send email - complete.")
            Else
                Console.Write("Unable to Send Email notification.")
                MyLogFile.WriteLine("Unable to Send Email notification.")
            End If
            Return True
        Catch ex As Exception
            Console.Write("Unable to Send Email notification.")
            MyLogFile.WriteLine("Unable to Send Email notification.")
        Finally
        End Try
    End Function



    Function SendErrorEmail(ByRef strResultMessage As String) As Boolean
        -- Reju Mathew 2010.12.15
        -- Read the tblReplyExportHistory  table and send emails as attachment.

        Dim strSendEmailTo As String = My.Settings.SendEmailTo
        Dim strFileName As String = ""
        Dim StrMessage As String = ""

        --Write into loag
        MyLogFile.WriteLine(strResultMessage)

        --email variables
        Dim strSendFrom As String = My.Settings.NotificationEmailSendFrom
        Dim strDefaultSubjectLine As String = My.Settings.DefaultErrorSubjectLine
        Dim strDefaultMessageBody As String = My.Settings.DefaultMessage
        Dim strReportFileName As String = ""

        Try
            StrMessage = strDefaultMessageBody + vbCrLf + "Error In applying Engagement data in IDMS tables.  Please check " + strGlobalLogFile + vbCrLf
            StrMessage = StrMessage + strResultMessage

            If Un_global.SendEMailUsingSMTP(strSendEmailTo, strSendFrom, strDefaultSubjectLine, StrMessage, strReportFileName, strResultMessage) Then
                Console.Write("Send email - complete.")
            Else
                MyLogFile.WriteLine("Unable to Send Email notification.")
                Console.Write("Unable to Send Email notification.")
            End If
            Return True
        Catch ex As Exception
            Console.Write("Unable to Send Email notification.")
            MyLogFile.WriteLine("Unable to Send Email notification.")
        Finally
        End Try
    End Function


End Module








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

