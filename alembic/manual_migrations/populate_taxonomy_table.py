    # Insert taxonomies table
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (0, 'NULL', 'NULL', 'NULL', 'False', 'NULL', 'NULL', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (212017, 'Lethocerus indicus', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=212017', 'NCBI:txid212017', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (31604, 'Small ruminant morbillivirus', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=31604', 'NCBI:txid31604', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (3702, 'Arabidopsis thaliana', 'thale cress', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=3702', 'NCBI:txid3702', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (559292, 'Saccharomyces cerevisiae S288C', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=559292', 'NCBI:txid559292', 6, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (254252, 'Lactococcus virus P2', 'NULL', 'SPECIES', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=254252', 'NCBI:txid254252', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1521, 'Ruminiclostridium cellulolyticum', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1521', 'NCBI:txid1521', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (9913, 'Bos taurus', 'cattle', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9913', 'NCBI:txid9913', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (9823, 'Sus scrofa', 'pig', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9823', 'NCBI:txid9823', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (223997, 'Murine norovirus 1', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=223997', 'NCBI:txid223997', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (70601, 'Pyrococcus horikoshii OT3', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=70601', 'NCBI:txid70601', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (10710, 'Lambdavirus lambda', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=10710', 'NCBI:txid10710', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (10754, 'Lederbergvirus P22', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=10754', 'NCBI:txid10754', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (271848, 'Burkholderia thailandensis E264', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=271848', 'NCBI:txid271848', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (9606, 'Homo sapiens', 'human', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9606', 'NCBI:txid9606', 5, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (392021, 'Rickettsia rickettsii str. ''Sheila Smith''', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=392021', 'NCBI:txid392021', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (294381, 'Entamoeba histolytica HM-1:IMSS', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=294381', 'NCBI:txid294381', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (90371, 'Salmonella enterica subsp. enterica serovar Typhimurium', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=90371', 'NCBI:txid90371', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (211044, 'Influenza A virus (A/Puerto Rico/8/1934(H1N1))', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=211044', 'NCBI:txid211044', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (83332, 'Mycobacterium tuberculosis H37Rv', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=83332', 'NCBI:txid83332', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (226185, 'Enterococcus faecalis V583', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=226185', 'NCBI:txid226185', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (3055, 'Chlamydomonas reinhardtii', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=3055', 'NCBI:txid3055', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (7227, 'Drosophila melanogaster', 'fruit fly', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=7227', 'NCBI:txid7227', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (196620, 'Staphylococcus aureus subsp. aureus MW2', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=196620', 'NCBI:txid196620', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (103690, 'Nostoc sp. PCC 7120 = FACHB-418', 'NULL', 'SPECIES', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=103690', 'NCBI:txid103690', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (85962, 'Helicobacter pylori 26695', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=85962', 'NCBI:txid85962', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (224308, 'Bacillus subtilis subsp. subtilis str. 168', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=224308', 'NCBI:txid224308', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (262724, 'Thermus thermophilus HB27', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=262724', 'NCBI:txid262724', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1280, 'Staphylococcus aureus', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1280', 'NCBI:txid1280', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (31634, 'Dengue virus 2 Thailand/16681/84', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=31634', 'NCBI:txid31634', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (4784, 'Phytophthora capsici', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=4784', 'NCBI:txid4784', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (195102, 'Clostridium perfringens str. 13', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=195102', 'NCBI:txid195102', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1260, 'Finegoldia magna', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1260', 'NCBI:txid1260', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (170187, 'Streptococcus pneumoniae TIGR4', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=170187', 'NCBI:txid170187', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (8201, 'Lycodichthys dearborni', 'Antarctic eel pout', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=8201', 'NCBI:txid8201', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (83333, 'Escherichia coli K-12', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=83333', 'NCBI:txid83333', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (94237, 'Mola mola', 'ocean sunfish', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=94237', 'NCBI:txid94237', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1351, 'Enterococcus faecalis', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1351', 'NCBI:txid1351', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (224324, 'Aquifex aeolicus VF5', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=224324', 'NCBI:txid224324', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (272560, 'Burkholderia pseudomallei K96243', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=272560', 'NCBI:txid272560', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1422, 'Geobacillus stearothermophilus', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1422', 'NCBI:txid1422', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (203119, 'Acetivibrio thermocellus ATCC 27405', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=203119', 'NCBI:txid203119', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (258594, 'Rhodopseudomonas palustris CGA009', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=258594', 'NCBI:txid258594', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (269799, 'Geobacter metallireducens GS-15', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=269799', 'NCBI:txid269799', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (287, 'Pseudomonas aeruginosa', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=287', 'NCBI:txid287', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (32049, 'Synechococcus sp. PCC 7002', 'NULL', 'SPECIES', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=32049', 'NCBI:txid32049', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (64320, 'Zika virus', 'NULL', 'NULL', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=64320', 'NCBI:txid64320', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (274, 'Thermus thermophilus', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=274', 'NCBI:txid274', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (187420, 'Methanothermobacter thermautotrophicus str. Delta H', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=187420', 'NCBI:txid187420', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1961, 'Streptomyces virginiae', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1961', 'NCBI:txid1961', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (48416, 'Zoarces viviparus', 'viviparous blenny', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=48416', 'NCBI:txid48416', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (316385, 'Escherichia coli str. K-12 substr. DH10B', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=316385', 'NCBI:txid316385', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (387139, 'Influenza A virus (A/Aichi/2/1968(H3N2))', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=387139', 'NCBI:txid387139', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (351614, 'Xenorhabdus stockiae', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=351614', 'NCBI:txid351614', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (93061, 'Staphylococcus aureus subsp. aureus NCTC 8325', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=93061', 'NCBI:txid93061', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (232237, 'Xipdecavirus Xp10', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=232237', 'NCBI:txid232237', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (272620, 'Klebsiella pneumoniae subsp. pneumoniae MGH 78578', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=272620', 'NCBI:txid272620', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (562, 'Escherichia coli', 'E. coli', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=562', 'NCBI:txid562', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (9031, 'Gallus gallus', 'chicken', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=9031', 'NCBI:txid9031', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (282458, 'Staphylococcus aureus subsp. aureus MRSA252', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=282458', 'NCBI:txid282458', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (169963, 'Listeria monocytogenes EGD-e', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=169963', 'NCBI:txid169963', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (469008, 'Escherichia coli BL21(DE3)', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=469008', 'NCBI:txid469008', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (114727, 'H1N1 subtype', 'NULL', 'SEROTYPE', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=114727', 'NCBI:txid114727', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (463722, 'Murine norovirus GV/CR6/2005/USA', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=463722', 'NCBI:txid463722', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (211586, 'Shewanella oneidensis MR-1', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=211586', 'NCBI:txid211586', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (8199, 'Zoarces americanus', 'ocean pout', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=8199', 'NCBI:txid8199', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (218491, 'Pectobacterium atrosepticum SCRI1043', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=218491', 'NCBI:txid218491', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (484895, 'Bruynoghevirus LUZ24', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=484895', 'NCBI:txid484895', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (44689, 'Dictyostelium discoideum', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=44689', 'NCBI:txid44689', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (10116, 'Rattus norvegicus', 'Norway rat', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=10116', 'NCBI:txid10116', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (194439, 'Chlorobaculum tepidum TLS', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=194439', 'NCBI:txid194439', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (10760, 'Escherichia phage T7', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=10760', 'NCBI:txid10760', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (523795, 'Porcine enteric sapovirus swine/Cowden/1980/US', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=523795', 'NCBI:txid523795', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (6239, 'Caenorhabditis elegans', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=6239', 'NCBI:txid6239', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (6100, 'Aequorea victoria', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=6100', 'NCBI:txid6100', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (33113, 'Atropa belladonna', 'belladonna', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=33113', 'NCBI:txid33113', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (29413, 'Nostoc sp. PCC 8009', 'NULL', 'SPECIES', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=29413', 'NCBI:txid29413', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (243232, 'Methanocaldococcus jannaschii DSM 2661', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=243232', 'NCBI:txid243232', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1320, 'Streptococcus sp. ''group G''', 'NULL', 'SPECIES', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1320', 'NCBI:txid1320', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (273057, 'Saccharolobus solfataricus P2', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=273057', 'NCBI:txid273057', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (34, 'Myxococcus xanthus', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=34', 'NCBI:txid34', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (99287, 'Salmonella enterica subsp. enterica serovar Typhimurium str. LT2', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=99287', 'NCBI:txid99287', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (291231, 'Zoarces elongatus', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=291231', 'NCBI:txid291231', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (2681611, 'Escherichia phage Lambda', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=2681611', 'NCBI:txid2681611', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1314, 'Streptococcus pyogenes', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1314', 'NCBI:txid1314', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (246200, 'Ruegeria pomeroyi DSS-3', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=246200', 'NCBI:txid246200', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (195103, 'Clostridium perfringens ATCC 13124', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=195103', 'NCBI:txid195103', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1185654, 'Pyrococcus furiosus COM1', 'NULL', 'STRAIN', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1185654', 'NCBI:txid1185654', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1515, 'Acetivibrio thermocellus', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1515', 'NCBI:txid1515', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (2697049, 'Severe acute respiratory syndrome coronavirus 2', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=2697049', 'NCBI:txid2697049', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (10090, 'Mus musculus', 'house mouse', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=10090', 'NCBI:txid10090', 8, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (1111708, 'Synechocystis sp. PCC 6803 substr. Kazusa', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=1111708', 'NCBI:txid1111708', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (10712, 'Phage 434', 'NULL', 'NULL', 'False', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=10712', 'NCBI:txid10712', NULL, NOW(), NOW())"""
    )
    op.execute(
        """INSERT INTO taxonomies (tax_id, organism_name, common_name, rank, has_described_species_name, url, article_reference, genome_identifier_id, creation_date, modification_date) VALUES (37553, 'Saprolegnia monoica', 'NULL', 'SPECIES', 'True', 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=37553', 'NCBI:txid37553', NULL, NOW(), NOW())"""
    )
