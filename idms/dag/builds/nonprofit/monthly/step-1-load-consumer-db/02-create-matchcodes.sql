
UPDATE {maintable_name}
  SET Individual_MC = GETMATCHCODE(firstname, lastname, AddressLine1, Zip, '', 'I'),
        Company_MC  = GETMATCHCODE(firstname, lastname, AddressLine1, Zip, lastname, 'C'),
        flatitude = CAST(ISNULL(LEFT(RIGHT('000' + CAST(CAST(Latitude_V as INT) as VARCHAR),9),3) + '.' + RIGHT(RIGHT('000' + CAST(CAST(Latitude_V as INT) as VARCHAR),9),6), '0') as float),
        flongitude = CAST(ISNULL(LEFT(RIGHT('000' + CAST(CAST(Longitude_V as INT) as VARCHAR),9),3) + '.' + RIGHT(RIGHT('000' + CAST(CAST(Longitude_V as INT) as VARCHAR),9),6), '0') as float);

