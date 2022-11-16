rem
rem file copy script  Author Keru Kannan Version 1.0
net use * /delete /Y
net use V: \\stcdmmclus\NextMarkExtracts

V:

cd awsnextmark
rem aws s3 mv s3://axle-internal-sources/raw/nextmark/inpfinanceall/DMM_Report_Nordic_FinanceAll.csv s3://axle-internal-sources/raw/nextmark/inpfinanceall/backup/DMM_Report_Nordic_FinanceAll.old
rem aws s3 mv s3://axle-internal-sources/raw/nextmark/listorder/DMM_INP_inpfinanceall.csv s3://axle-internal-sources/raw/nextmark/listorder/backup/DMM_INP_inpfinanceall.csv.old

aws s3 cp DMM_Report_Nordic_FinanceAll.csv.gz s3://axle-internal-sources/raw/nextmark/inpfinanceall/DMM_Report_Nordic_FinanceAll.csv.gz

aws s3 cp DMM_Report_Nordic_ListOrder.csv.gz s3://axle-internal-sources/raw/nextmark/listorder/DMM_Report_Nordic_ListOrder.csv.gz
c:
