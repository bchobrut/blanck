#Utility for aggregating multiple gzipped maf files downloaded from gdc into a single file
from __future__ import print_function, division, absolute_import
import logging
import os
import gzip
import pandas
from pandas import isnull
from tqdm import tqdm


#set location of maf files
maf_path = "/mnt/usfboxsync/Blanck Collabs/Mickie_SARC/target/maf/"

#set output location
output_path = "/mnt/usfboxsync/Blanck Collabs/Mickie_SARC/target/"

#set whether to include SNPs (germline mutations) or not
snp = False

TCGA_PATIENT_ID_LENGTH = 12

MAF_COLUMN_NAMES = [
    'Hugo_Symbol',
    'Entrez_Gene_Id',
    'Center',
    'NCBI_Build',
    'Chromosome',
    'Start_Position',
    'End_Position',
    'Strand',
    'Variant_Classification',
    'Variant_Type',
    'Reference_Allele',
    'Tumor_Seq_Allele1',
    'Tumor_Seq_Allele2',
    'dbSNP_RS',
    'dbSNP_Val_Status',
    'Tumor_Sample_Barcode',
    'Matched_Norm_Sample_Barcode',
    'Match_Norm_Seq_Allele1',
    'Match_Norm_Seq_Allele2',
]


def load_maf_dataframe(path, nrows=None, raise_on_error=True, encoding=None):
    """
    Load the guaranteed columns of a TCGA MAF file into a DataFrame
    Parameters
    ----------
    path : str
        Path to MAF file
    nrows : int
        Optional limit to number of rows loaded
    raise_on_error : bool
        Raise an exception upon encountering an error or log an error
    encoding : str, optional
        Encoding to use for UTF when reading MAF file.
    """

    n_basic_columns = len(MAF_COLUMN_NAMES)

    # pylint: disable=no-member
    # pylint gets confused by read_csv
    df = pandas.read_csv(
        path,
        comment="#",
        sep="\t",
        low_memory=False,
        skip_blank_lines=True,
        header=0,
        nrows=nrows,
        encoding=encoding)

    if len(df.columns) < n_basic_columns:
        error_message = (
            "Too few columns in MAF file %s, expected %d but got  %d : %s" % (
                path, n_basic_columns, len(df.columns), df.columns))
        if raise_on_error:
            raise ValueError(error_message)
        else:
            logging.warn(error_message)

    # check each pair of expected/actual column names to make sure they match
    for expected, actual in zip(MAF_COLUMN_NAMES, df.columns):
        if expected != actual:
            # MAFs in the wild have capitalization differences in their
            # column names, normalize them to always use the names above
            if expected.lower() == actual.lower():
                # using DataFrame.rename in Python 2.7.x doesn't seem to
                # work for some files, possibly because Pandas treats
                # unicode vs. str columns as different?
                df[expected] = df[actual]
                del df[actual]
            else:
                error_message = (
                    "Expected column %s but got %s" % (expected, actual))
                if raise_on_error:
                    raise ValueError(error_message)
                else:
                    logging.warn(error_message)

    return df#[MAF_COLUMN_NAMES]

#loop for aggregating mafs
final_df = pandas.DataFrame()
for root, dirs, files in tqdm(os.walk(maf_path, topdown=False)):
    for name in files:
        if "parcel" in name:
            continue
        with gzip.open(os.path.join(root, name)) as f:
            df = load_maf_dataframe(f)
            if snp == False:
                df = df[df["Variant_Type"] != "SNP"]
            final_df = pandas.concat([final_df, df], ignore_index=True)
            
final_df.to_csv(os.path.join(output_path, "mutations.csv"), index=False)