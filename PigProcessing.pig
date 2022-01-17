

-- loading & filtering using PIG 

-- to use csvexcelstorage register piggybank.jar to use the different modules in piggybank
--REGISTER gs://dataproc-staging-us-central1-51617460704-vfhcwe8k/JAR/jar_files/piggybank-0.17.0.jar;
--PigInputFile = LOAD 'gs://dataproc-staging-us-central1-51617460704-vfhcwe8k/CT_Assignment_1/Colaborated_UnFiltered2.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS(Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:datetime, DeletionDate:datetime, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:datetime, LastActivityDate:datetime, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:datetime, CommunityOwnedDate:datetime, ContentLicense:chararray);

-- Loading the 200K posts data from the cluster cloud storage bucket(from task 1)
PigInputFile = LOAD 'gs://dataproc-staging-us-central1-51617460704-vfhcwe8k/CT_Assignment_1/Colaborated_Unfiltered_data.csv' USING PigStorage(',');

-- choose the columns needed from the complete dataset
columnFile = FOREACH PigInputFile GENERATE $0 AS Id:int, $6 AS Score:int, $7 AS ViewCount:int, $8 AS UserId:int, $14 AS Title:chararray, $15 AS Tags:chararray;

-- remove the special characters from body 
FilterSpecialChar = FOREACH columnFile GENERATE Id,Score,ViewCount,UserId,Title,Tags, REPLACE(Body, '<.*?.>', '') AS Body;

-- remove new lines from body 
FilterFinal = FOREACH FilterSpecialChar GENERATE Id,Score,ViewCount,UserId,Title,Tags, REPLACE(Body, '\n', ' ') AS Body;

--Remove header 
HeaderRemoved= FILTER columnFile BY $4 != 'Title'

-- excludig rows without User Id 
PigOutputFile = FILTER FilterFinal BY (UserId IS NOT NULL);

-- Store back the processed file to storage bucket 
STORE PigOutputFile INTO 'gs://dataproc-staging-us-central1-51617460704-vfhcwe8k/CT_Assignment_1/PigOutputFile.csv' USING PigStorage(',');


-- output -This reads 200k recods and stores 194802