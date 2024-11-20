study_uid=${1}

personal_dir=$(pwd)
mkdir /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_v01

mv /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}.xlsx /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_v01

cd /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_v01/
pwd 
python3 /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/GEOFF_code/geoff_validation/GEOFF_tools_v02.py excel_extract --excel /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_v01/${study_uid}.xlsx --yaml /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/GEOFF_code/geoff_validation/GEOFF_tools_parameters.yaml >> /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_v01/${study_uid}_extract.out 2>/nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_v01/${study_uid}_extract.err

python3 /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/GEOFF_code/geoff_validation/GEOFF_tools_v02.py tsv_validate --study_tsv /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_v01/output_extract_tsv/${study_uid}_v01_study_data.tsv --yaml /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/GEOFF_code/geoff_validation/GEOFF_tools_parameters.yaml >> /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_v01/${study_uid}_validate.out 2>/nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_v01/${study_uid}_validate.err

#cd /nfs/jbailey5/baileyweb/bailey_share/GEOFF_META/studies/${study_uid}_01/
mkdir output_extract_tsv/${study_uid}_valid
mv output_extract_tsv/*validated* output_extract_tsv/${study_uid}_valid
gzip -r output_extract_tsv/${study_uid}_valid
