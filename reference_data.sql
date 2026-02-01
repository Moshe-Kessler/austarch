-- =============================================================================
-- AustArch Reference Data
-- Seed data for reference tables
-- =============================================================================

-- =============================================================================
-- DATING METHODS
-- =============================================================================

INSERT INTO dating_method (code, name, description, is_radiometric) VALUES
('C14', 'Radiocarbon', 'Carbon-14 dating measuring radioactive decay of 14C', TRUE),
('AMS', 'AMS Radiocarbon', 'Accelerator Mass Spectrometry radiocarbon dating', TRUE),
('CONV', 'Conventional Radiocarbon', 'Conventional/radiometric radiocarbon counting', TRUE),
('OSL', 'Optically Stimulated Luminescence', 'Luminescence dating using optical stimulation', TRUE),
('TL', 'Thermoluminescence', 'Luminescence dating using thermal stimulation', TRUE),
('IRSL', 'Infrared Stimulated Luminescence', 'Luminescence dating using infrared stimulation', TRUE),
('U-TH', 'Uranium-Thorium', 'U-series dating measuring uranium decay to thorium', TRUE),
('U-PB', 'Uranium-Lead', 'U-series dating measuring uranium decay to lead', TRUE),
('ESR', 'Electron Spin Resonance', 'ESR dating measuring unpaired electrons', TRUE),
('AAR', 'Amino Acid Racemization', 'Dating based on amino acid racemization rates', FALSE),
('COSMO', 'Cosmogenic Nuclide', 'Surface exposure dating using cosmogenic isotopes', TRUE),
('K-AR', 'Potassium-Argon', 'Dating based on potassium-40 decay to argon-40', TRUE),
('AR-AR', 'Argon-Argon', 'Refined potassium-argon dating method', TRUE),
('PALEO', 'Palaeomagnetic', 'Dating using Earth magnetic field reversals', FALSE),
('TEPHRA', 'Tephrochronology', 'Dating using volcanic ash layers', FALSE),
('OTHER', 'Other/Unknown', 'Other or unspecified dating method', FALSE);

-- =============================================================================
-- SAMPLE MATERIALS
-- =============================================================================

INSERT INTO sample_material (code, name, category, suitable_for_c14, notes) VALUES
-- Organic materials
('CHARCOAL', 'Charcoal', 'organic', TRUE, 'Most reliable material for radiocarbon dating'),
('WOOD', 'Wood', 'organic', TRUE, 'May have old wood effect'),
('BONE', 'Bone', 'organic', TRUE, 'Requires collagen extraction'),
('BONE_BURNT', 'Burnt Bone', 'organic', TRUE, 'Carbonized bone material'),
('SHELL_MARINE', 'Marine Shell', 'organic', TRUE, 'Requires marine reservoir correction'),
('SHELL_FRESHWATER', 'Freshwater Shell', 'organic', TRUE, 'May have hardwater effect'),
('SHELL_TERRESTRIAL', 'Terrestrial Shell', 'organic', TRUE, 'Land snail shell'),
('SHELL_UNSPEC', 'Shell (unspecified)', 'organic', TRUE, 'Shell type not specified'),
('SEED', 'Seeds/Plant Remains', 'organic', TRUE, 'Short-lived plant material'),
('PEAT', 'Peat', 'organic', TRUE, 'Organic sediment'),
('SOIL_ORG', 'Organic Soil/Sediment', 'organic', TRUE, 'Soil organic fraction'),
('HAIR', 'Hair/Fur', 'organic', TRUE, 'Keratin material'),
('EGGSHELL', 'Eggshell', 'organic', TRUE, 'Avian eggshell'),
('RESIN', 'Resin/Gum', 'organic', TRUE, 'Plant exudates'),
('FIBER', 'Plant Fiber', 'organic', TRUE, 'Cordage, basketry, etc.'),
('DUNG', 'Dung/Coprolite', 'organic', TRUE, 'Preserved fecal material'),
('CORAL', 'Coral', 'organic', TRUE, 'Suitable for U-series dating'),

-- Inorganic materials (for luminescence)
('QUARTZ', 'Quartz', 'inorganic', FALSE, 'OSL dating of quartz grains'),
('FELDSPAR', 'Feldspar', 'inorganic', FALSE, 'IRSL dating of feldspar'),
('SEDIMENT', 'Sediment', 'inorganic', FALSE, 'Bulk sediment for luminescence'),
('SAND', 'Sand', 'inorganic', FALSE, 'Aeolian or fluvial sand'),
('CALCITE', 'Calcite/Calcium Carbonate', 'inorganic', FALSE, 'Speleothem, tufa'),
('TOOTH_ENAMEL', 'Tooth Enamel', 'inorganic', FALSE, 'ESR or U-series dating'),
('HEARTH', 'Hearth Material', 'mixed', TRUE, 'Burnt earth or stones from hearth'),
('CERAMIC', 'Ceramic/Pottery', 'inorganic', FALSE, 'TL dating of fired clay'),

-- General/Unknown
('MIXED', 'Mixed Materials', 'mixed', TRUE, 'Multiple material types'),
('OTHER', 'Other', 'other', TRUE, 'Other or unspecified material'),
('UNKNOWN', 'Unknown', 'unknown', TRUE, 'Material not recorded');

-- =============================================================================
-- IBRA BIOREGIONS (Seed data - full geometry would come from shapefile)
-- =============================================================================

-- This is a reference list of all 89 IBRA 7.0 bioregions
-- Full geometry data should be loaded from official IBRA shapefile

INSERT INTO bioregion (ibra_code, name, state) VALUES
-- Australian Capital Territory
('AUA', 'Australian Alps', 'ACT/NSW/VIC'),

-- New South Wales
('BBN', 'Brigalow Belt North', 'NSW/QLD'),
('BBS', 'Brigalow Belt South', 'NSW/QLD'),
('BHC', 'Broken Hill Complex', 'NSW'),
('CHC', 'Channel Country', 'NSW/QLD/SA'),
('COP', 'Cobar Peneplain', 'NSW'),
('DRP', 'Darling Riverine Plains', 'NSW'),
('MDD', 'Murray Darling Depression', 'NSW/SA/VIC'),
('NAN', 'Nandewar', 'NSW'),
('NET', 'New England Tablelands', 'NSW'),
('NSS', 'NSW South Western Slopes', 'NSW'),
('RIV', 'Riverina', 'NSW/VIC'),
('SEC', 'South East Corner', 'NSW/VIC'),
('SEH', 'South Eastern Highlands', 'NSW/ACT/VIC'),
('SSD', 'Simpson Strzelecki Dunefields', 'NSW/QLD/SA'),
('SYB', 'Sydney Basin', 'NSW'),

-- Northern Territory
('ARC', 'Arnhem Coast', 'NT'),
('ARP', 'Arnhem Plateau', 'NT'),
('CEK', 'Central Kimberley', 'NT/WA'),
('CER', 'Central Ranges', 'NT/SA'),
('DAB', 'Darwin Coastal', 'NT'),
('DAC', 'Daly Basin', 'NT'),
('DMR', 'Davenport Murchison Ranges', 'NT'),
('FIN', 'Finke', 'NT'),
('GAS', 'Great Artesian Basin (NT)', 'NT'),
('GFU', 'Gulf Fall and Uplands', 'NT/QLD'),
('GSD', 'Great Sandy Desert', 'NT/WA'),
('GUC', 'Gulf Coastal', 'NT/QLD'),
('GUP', 'Gulf Plains', 'NT/QLD'),
('MAC', 'MacDonnell Ranges', 'NT'),
('MGD', 'Mitchell Grass Downs', 'NT/QLD'),
('OVP', 'Ord Victoria Plain', 'NT/WA'),
('PCK', 'Pine Creek', 'NT'),
('STU', 'Sturt Plateau', 'NT'),
('TAN', 'Tanami', 'NT/WA'),
('TIW', 'Tiwi Cobourg', 'NT'),
('VIB', 'Victoria Bonaparte', 'NT/WA'),

-- Queensland
('BRT', 'Brigalow Belt North', 'QLD'),
('CAP', 'Cape York Peninsula', 'QLD'),
('CQC', 'Central Queensland Coast', 'QLD'),
('DEU', 'Desert Uplands', 'QLD'),
('EIU', 'Einasleigh Uplands', 'QLD'),
('MII', 'Mount Isa Inlier', 'QLD'),
('MUL', 'Mulga Lands', 'QLD'),
('NWH', 'Northwest Highlands', 'QLD'),
('SEQ', 'South East Queensland', 'QLD'),
('WET', 'Wet Tropics', 'QLD'),

-- South Australia
('AWT', 'Avon Wheatbelt', 'SA/WA'),
('CHR', 'Coolgardie', 'SA/WA'),
('EPL', 'Eyre Yorke Block', 'SA'),
('FLB', 'Flinders Lofty Block', 'SA'),
('GAW', 'Gawler', 'SA'),
('HAM', 'Hampton', 'SA'),
('KAN', 'Kanmantoo', 'SA'),
('NCP', 'Naracoorte Coastal Plain', 'SA/VIC'),
('NUL', 'Nullarbor', 'SA/WA'),
('STP', 'Stony Plains', 'SA'),

-- Tasmania
('BEL', 'Ben Lomond', 'TAS'),
('FUR', 'Furneaux', 'TAS'),
('KIN', 'King', 'TAS'),
('TCH', 'Tasmanian Central Highlands', 'TAS'),
('TNM', 'Tasmanian Northern Midlands', 'TAS'),
('TNS', 'Tasmanian Northern Slopes', 'TAS'),
('TSE', 'Tasmanian South East', 'TAS'),
('TSR', 'Tasmanian Southern Ranges', 'TAS'),
('TWE', 'Tasmanian West', 'TAS'),

-- Victoria
('GIP', 'Gippsland Plain', 'VIC'),
('NCV', 'NSW North Coast', 'VIC/NSW'),
('SCP', 'South East Coastal Plain', 'VIC'),
('VIM', 'Victorian Midlands', 'VIC'),
('VVP', 'Victorian Volcanic Plain', 'VIC'),

-- Western Australia
('AVW', 'Avon Wheatbelt', 'WA'),
('CAR', 'Carnarvon', 'WA'),
('COO', 'Coolgardie', 'WA'),
('ESP', 'Esperance Plains', 'WA'),
('GER', 'Geraldton Sandplains', 'WA'),
('GGI', 'Gibson Desert', 'WA'),
('GVD', 'Great Victoria Desert', 'WA'),
('JAF', 'Jarrah Forest', 'WA'),
('LSD', 'Little Sandy Desert', 'WA'),
('MAL', 'Mallee', 'WA/SA/VIC'),
('MUR', 'Murchison', 'WA'),
('NKB', 'Northern Kimberley', 'WA'),
('PIL', 'Pilbara', 'WA'),
('SWA', 'Swan Coastal Plain', 'WA'),
('WAR', 'Warren', 'WA'),
('YAL', 'Yalgoo', 'WA');

-- Note: IBRA geometry should be loaded from official shapefile using:
-- shp2pgsql -I -s 4283 IBRA7_regions.shp bioregion_geom | psql -d austarch
-- Then update bioregion table with geometry:
-- UPDATE bioregion b SET geom = g.geom
-- FROM bioregion_geom g WHERE b.ibra_code = g.reg_code_7;

-- =============================================================================
-- END OF REFERENCE DATA
-- =============================================================================
