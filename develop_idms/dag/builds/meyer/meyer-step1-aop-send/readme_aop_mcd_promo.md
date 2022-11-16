# MCD AOP and PromoHistory Processing Requirements
### MEYER-AOP-Step1
- Step 1: Export 100/Previous tblMain to WB
- Step 2: Export 200/Current tblMain to WB

###MEYER-EHYG-Step1
- Step 3: Export Emails for hygiene

### MEYER-AOP-Step2
- Step 4: Import of 100+200 WB Output into RedShift
- Step 5: Update current tblMain with Data from 100+200 WB Output WHERE FileCode = 200
- Step 6: Export MCD File as per MCD Input Layout - -(s3://idms-7933-aop-input/IDMSAOPPI1/a05_meyer_mcd_input.txt)

### MEYER-AOP-Step3
- Step 7: Import MCD Output and update current tblMain.MCD INDIVIDUAL ID & MCD HOUSEHOLD ID for FILE_NO = 200. No update to Previous tblMain - Pending

### MEYER (builds)
- Step 8: Run all custom fields/aggregates SQL
- Step 9: Run Model Script

###MEYER-EHYG-Step2
- Step 10: Run e-hygiene


### MEYER - Helper
- Step 7: Import MCD Myer_MCD_Output_XRef. MERGE into existing table. We will not truncate and load - Ram
- Step 8: Step and UPDATE each Count_NNNNN table with new Individual ID and Household_ID. Exclude "No Usage" campaigns (Ram) - Pending
