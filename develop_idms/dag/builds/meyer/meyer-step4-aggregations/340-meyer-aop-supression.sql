/*
18.3 Mailable Flag
Set the MAILABLE field based on the AH1 MAILABILITY SCORE and the suppression flags.
IF AH1 MAILABILITY SCORE is in (4,5) or any of the following suppression flags are ‘Y’,
set the MAILABLE flag to ‘N’, otherwise set to ‘Y’.
    Suppression Flags to Consider for the ‘N’ value are:
    DMA Mail Preference Service Suppression Indicator - AH1 SUPP DMA MAIL PREF IND
    USPS Complaint Suppression Indicator - SUPP USPS CMPLNT IND
    Business Suppression Indicator - SUPP BUS IND
    Data Axle Mail Complaint Suppression Indicator 1 - SUPP IG MAIL CMPLNT 1 IND
    Deceased Suppression Indicator - SUPP DECEASED IND
    Relative Reported Deceased Indicator - SUPP DECEASED REL IND
    Data Axle 3rd Party Suppression Indicator - SUPP 3RD PRTY IND
    Retirement Home Suppression Indicator - SUPP RETIRE HOME IND
    Nursing Home Indicator Suppression Indicator - SUPP NURSING HOME IND
    Data Axle Mail Complaint Suppression Indicator 2- SUPP IG MAIL CMPLNT 2 IND
    Prison Suppression Indicator –SUPP PRISON IND
    Attorney General Suppression Indicator - SUPP ATRNY GEN IND
    Data Axle Business Phone Suppression Indicator - SUPP IG BUS PHONE IND
    Email Suppression Indicator - SUPP EMAIL IND
*/
--20210816 CB: cannot set to null, may have N values from aggregations step
--update {maintable_name} set mailable = null;

update {maintable_name}
   set mailable = case when left(ah1mailabilityscore,1) in ('4','5') then 'N' else 'Y' end
where nvl(mailable,'')='';

update {maintable_name}
   set mailable = 'N'
where mailable = 'Y'
   and (supp1mpspander = 'Y'
       or supp1offpander = 'Y'
       or supp1buspander = 'Y'
       or supp1fdspander = 'Y'
       or supp1decpander = 'Y'
       or supp1relpander = 'Y'
       or supp1extpander = 'Y'
       or supp1retpander = 'Y'
       or supp1nurpander = 'Y'
       or supp1dbapander = 'Y'
       or supp1acapander = 'Y'
       or supp1attgenfile = 'Y'
       or supp1infobusphone = 'Y'
       or supp1emailsupp = 'Y');

