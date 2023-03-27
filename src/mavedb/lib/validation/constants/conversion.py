aa_dict_key_1 = {
    "A": "Ala", "C": "Cys", "D": "Asp", "E": "Glu", "F": "Phe",
    "G": "Gly", "H": "His", "I": "Ile", "K": "Lys", "L": "Leu",
    "M": "Met", "N": "Asn", "P": "Pro", "Q": "Gln", "R": "Arg",
    "S": "Ser", "T": "Thr", "V": "Val", "W": "Trp", "Y": "Tyr",
    "*": "Ter"
}  # what is Z? "X":"Ter", "WTSYN":""}

aa_dict_key_3 = {
    "Ala": "A", "Cys": "C", "Asp": "D", "Glu": "E", "Phe": "F",
    "Gly": "G", "His": "H", "Ile": "I", "Lys": "K", "Leu": "L",
    "Met": "M", "Asn": "N", "Pro": "P", "Gln": "Q", "Arg": "R",
    "Ser": "S", "Thr": "T", "Val": "V", "Trp": "W", "Tyr": "Y",
    "Ter": "*"
}

codon_dict_DNA = {
    # T
    'TTT': 'Phe', 'TCT': 'Ser', 'TAT': 'Tyr', 'TGT': 'Cys',  # TxT
    'TTC': 'Phe', 'TCC': 'Ser', 'TAC': 'Tyr', 'TGC': 'Cys',  # TxC
    'TTA': 'Leu', 'TCA': 'Ser', 'TAA': 'Ter', 'TGA': 'Ter',  # TxA
    'TTG': 'Leu', 'TCG': 'Ser', 'TAG': 'Ter', 'TGG': 'Trp',  # TxG

    # C
    'CTT': 'Leu', 'CCT': 'Pro', 'CAT': 'His', 'CGT': 'Arg',  # CxT
    'CTC': 'Leu', 'CCC': 'Pro', 'CAC': 'His', 'CGC': 'Arg',  # CxC
    'CTA': 'Leu', 'CCA': 'Pro', 'CAA': 'Gln', 'CGA': 'Arg',  # CxA
    'CTG': 'Leu', 'CCG': 'Pro', 'CAG': 'Gln', 'CGG': 'Arg',  # CxG

    # A
    'ATT': 'Ile', 'ACT': 'Thr', 'AAT': 'Asn', 'AGT': 'Ser',  # AxT
    'ATC': 'Ile', 'ACC': 'Thr', 'AAC': 'Asn', 'AGC': 'Ser',  # AxC
    'ATA': 'Ile', 'ACA': 'Thr', 'AAA': 'Lys', 'AGA': 'Arg',  # AxA
    'ATG': 'Met', 'ACG': 'Thr', 'AAG': 'Lys', 'AGG': 'Arg',  # AxG

    # G
    'GTT': 'Val', 'GCT': 'Ala', 'GAT': 'Asp', 'GGT': 'Gly',  # GxT
    'GTC': 'Val', 'GCC': 'Ala', 'GAC': 'Asp', 'GGC': 'Gly',  # GxC
    'GTA': 'Val', 'GCA': 'Ala', 'GAA': 'Glu', 'GGA': 'Gly',  # GxA
    'GTG': 'Val', 'GCG': 'Ala', 'GAG': 'Glu', 'GGG': 'Gly'   # GxG
}

