"""
@summary: This module contains constants used for EML generation
@author: CJ Grady
@version: 2.1.1
@note: We are currently using EML version 2.1.1

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
from LmServer.common.lmconstants import FeatureNames

EML_NAMESPACE = "eml://ecoinformatics.org/eml-2.1.1"
EML_SCHEMA_LOCATION = "https://code.ecoinformatics.org/code/eml/tags/RELEASE_EML_2_1_1/eml.xsd"

EML_SYSTEM = "kubi.lifemapper"
EML_CUSTOM_UNITS = {
   "dimensionless": {
      "standard" : True,
      "standardUnit" : 'dimensionless'
   },
   "cm" : {
      "standard" : True,
      "standardUnit" : "centimeter"
   },
   "coefficientOfVariation" : {
      "standard" : False,
      "unitString" : "<stmml:unit xmlns:stmml='http://www.xml-cml.org/schema/stmml' id='coefficientOfVariation'><stmml:description>Coefficient of Variation</stmml:description></stmml:unit>"
   },
   "degreesCelsiusTimes10" : {
      "standard" : False,
      "unitString" : "<stmml:unit xmlns:stmml='http://www.xml-cml.org/schema/stmml' id='degreesCelsiusTimes10' parentSI='celsius' multiplierToSI='10'><stmml:description>10 times degrees celsius</stmml:description></stmml:unit>"
   },
   "degreesCelsiusTimes100" : {
      "standard" : False,
      "unitString" : "<stmml:unit xmlns:stmml='http://www.xml-cml.org/schema/stmml' id='degreesCelsiusTimes100' parentSI='celsius' multiplierToSI='100'><stmml:description>100 times degrees celsius</stmml:description></stmml:unit>"
   },
   "degrees" : {
      "standard" : True,
      "standardUnit" : "degree"
   },
   "meters" : {
      "standard" : True,
      "standardUnit" : "meter"
   },
   "mm" : {
      "standard" : True,
      "standardUnit" : "millimeter"
   },
   "standardDeviationTimes100" : {
      "standard" : False,
      "unitString" : "<stmml:unit xmlns:stmml='http://www.xml-cml.org/schema/stmml' id='standardDeviationTimes100'><stmml:description>Standard Deviation multiplied by 100</stmml:description></stmml:unit>"
   }
}

  
EML_KNOWN_FEATURES = {
   FeatureNames.CANONICAL_NAME: {
      "label" : "Canonical Name",
      "definition" : "This is the display name for the record.  It is often the name of the species.",
      "storageType" : "string",
      "measurementScale" : """\
               <measurementScale>
                  <nominal>
                     <nonNumericDomain>
                        <textDomain>
                           <definition>string</definition>
                        </textDomain>
                     </nonNumericDomain>
                  </nominal>
               </measurementScale>
"""
   },
   FeatureNames.CATALOG_NUMBER: {
      "label" : "Catalog Number",
      "definition" : "This is the number in the collection that houses the specimen.",
      "storageType" : "string",
      "measurementScale" : """\
               <measurementScale>
                  <nominal>
                     <nonNumericDomain>
                        <textDomain>
                           <definition>string</definition>
                        </textDomain>
                     </nonNumericDomain>
                  </nominal>
               </measurementScale>
"""
   },
   FeatureNames.COLLECTOR: {
      "label" : "Collector",
      "definition" : "The person that collected this specimen.",
      "storageType" : "string",
      "measurementScale" : """\
               <measurementScale>
                  <nominal>
                     <nonNumericDomain>
                        <textDomain>
                           <definition>string</definition>
                        </textDomain>
                     </nonNumericDomain>
                  </nominal>
               </measurementScale>
"""
   },
   FeatureNames.COLLECTION_CODE: {
      "label" : "Collection Code",
      "definition" : "The code associated with the collection holding the specimen",
      "storageType" : "string",
      "measurementScale" : """\
               <measurementScale>
                  <nominal>
                     <nonNumericDomain>
                        <textDomain>
                           <definition>string</definition>
                        </textDomain>
                     </nonNumericDomain>
                  </nominal>
               </measurementScale>
"""
   },
   FeatureNames.COLLECTION_DATE: {
      "label" : "Collection Date",
      "definition" : "The date the specimen was collected (in Modified Julian Day format)",
      "storageType" : "float",
      "measurementScale" : """\
               <measurementScale>
                  <interval>
                     <unit>
                        <standardUnit>nominalDay</standardUnit>
                     </unit>
                     <numericDomain>
                        <numberType>real</numberType>
                     </numericDomain>
                  </interval>
               </measurementScale>
"""
   },
   FeatureNames.INSTITUTION_CODE: {
      "label" : "Institution Code",
      "definition" : "A code representing the institution that collected the specimen",
      "storageType" : "string",
      "measurementScale" : """\
               <measurementScale>
                  <nominal>
                     <nonNumericDomain>
                        <textDomain>
                           <definition>string</definition>
                        </textDomain>
                     </nonNumericDomain>
                  </nominal>
               </measurementScale>
"""
   },
   FeatureNames.GBIF_LATITUDE: {
      "label" : "Latitude",
      "definition" : "The latitude portion of the point geometry",
      "storageType" : "float",
      "measurementScale" : """\
               <measurementScale>
                  <ratio>
                     <unit>
                        <standardUnit>degree</standardUnit>
                     </unit>
                     <numericDomain>
                        <numberType>real</numberType>
                     </numericDomain>
                  </ratio>
               </measurementScale>
"""
   },
   FeatureNames.GBIF_LONGITUDE: {
      "label" : "Longitude",
      "definition" : "The longitude portion of the point geometry",
      "storageType" : "float",
      "measurementScale" : """\
               <measurementScale>
                  <ratio>
                     <unit>
                        <standardUnit>degree</standardUnit>
                     </unit>
                     <numericDomain>
                        <numberType>real</numberType>
                     </numericDomain>
                  </ratio>
               </measurementScale>
"""
   },
   FeatureNames.MODIFICATION_DATE: {
      "label" : "Modification Date",
      "definition" : "The date (in Modified Julian Day format) this record was last modified",
      "storageType" : "float",
      "measurementScale" : """\
               <measurementScale>
                  <interval>
                     <unit>
                        <standardUnit>nominalDay</standardUnit>
                     </unit>
                     <numericDomain>
                        <numberType>real</numberType>
                     </numericDomain>
                  </interval>
               </measurementScale>
"""
   },
   FeatureNames.PROVIDER_NAME: {
      "label" : "Provider Name",
      "definition" : "The name of the provider of this record",
      "storageType" : "string",
      "measurementScale" : """\
               <measurementScale>
                  <nominal>
                     <nonNumericDomain>
                        <textDomain>
                           <definition>string</definition>
                        </textDomain>
                     </nonNumericDomain>
                  </nominal>
               </measurementScale>
"""
   },
   FeatureNames.RESOURCE_NAME: {
      "label" : "Resource Name",
      "definition" : "The name of the resource holding this record",
      "storageType" : "string",
      "measurementScale" : """\
               <measurementScale>
                  <nominal>
                     <nonNumericDomain>
                        <textDomain>
                           <definition>string</definition>
                        </textDomain>
                     </nonNumericDomain>
                  </nominal>
               </measurementScale>
"""
   },
   FeatureNames.URL: {
      "label" : "URL",
      "definition" : "A URL pointing at additional information for this record",
      "storageType" : "string",
      "measurementScale" : """\
               <measurementScale>
                  <nominal>
                     <nonNumericDomain>
                        <textDomain>
                           <definition>A URL pointing at additional information for this record</definition>
                        </textDomain>
                     </nonNumericDomain>
                  </nominal>
               </measurementScale>
"""
   },
   FeatureNames.LOCAL_ID: {
      "label" : "Local Id",
      "definition" : "A local identifier for this record",
      "storageType" : "string",
      "measurementScale" : """\
               <measurementScale>
                  <nominal>
                     <nonNumericDomain>
                        <textDomain>
                           <definition>string</definition>
                        </textDomain>
                     </nonNumericDomain>
                  </nominal>
               </measurementScale>
"""
   },
   FeatureNames.USER_LATITUDE: {
      "label" : "Latitude",
      "definition" : "The latitude portion of the point geometry",
      "storageType" : "float",
      "measurementScale" : """\
               <measurementScale>
                  <ratio>
                     <unit>
                        <standardUnit>degree</standardUnit>
                     </unit>
                     <numericDomain>
                        <numberType>real</numberType>
                     </numericDomain>
                  </ratio>
               </measurementScale>
"""
   },
   FeatureNames.USER_LONGITUDE: {
      "label" : "Longitude",
      "definition" : "The longitude portion of the point geometry",
      "storageType" : "float",
      "measurementScale" : """\
               <measurementScale>
                  <ratio>
                     <unit>
                        <standardUnit>degree</standardUnit>
                     </unit>
                     <numericDomain>
                        <numberType>real</numberType>
                     </numericDomain>
                  </ratio>
               </measurementScale>
"""
   }
}

# Standard units for EML 2.1.1, 
# from http://knb.ecoinformatics.org/software/eml/eml-2.1.1/eml-unitTypeDefinitions.html#StandardUnitDictionary
EML_STANDARD_UNITS = [
   #LengthUnitType
   "meter", "nanometer", "micrometer", "micron", "millimeter", "centimeter",
   "decimeter", "dekameter", "hectometer", "kilometer", "megameter", "angstrom",
   "inch", "Foot_US", "foot", "Foot_Gold_Coast", "fathom", "nauticalMile", 
   "yard", "Yard_Indian", "Link_Clarke", "Yard_Sears", "mile",
   #MassUnitType
   "kilogram", "nanogram", "microgram", "milligram", "centigram", "decigram",
   "gram", "dekagram", "hectogram", "megagram", "tonne", "pound", "ton",
   #otherUnitType
   "dimensionless", "second", "kelvin", "coulomb", "ampere", "mole", "candela", 
   "number", "radian", "degree", "grad", "cubicMeter", "nominalMinute", 
   "nominalHour", "nominalDay", "nominalWeek", "nominalYear", 
   "nominalLeapYear", "celsius", "fahrenheit", "nanosecond", "microsecond", 
   "millisecond", "centisecond", "decisecond", "dekasecond", "hectosecond", 
   "kilosecond", "megasecond", "minute", "hour", "kiloliter", "microliter", 
   "milliliter", "liter", "gallon", "quart", "bushel", "cubicInch", "pint", 
   "megahertz", "kilohertz", "hertz", "millihertz", "newton", "joule", 
   "calorie", "britishThermalUnit", "footPound", "lumen", "lux", "becquerel", 
   "gray", "sievert", "katal", "henry", "megawatt", "kilowatt", "watt", 
   "milliwatt", "megavolt", "kilovolt", "volt", "millivolt", "farad", "ohm", 
   "ohmMeter", "siemen", "weber", "tesla", "pascal", "megapascal", 
   "kilopascal", "atmosphere", "bar", "millibar", "kilogramsPerSquareMeter", 
   "gramsPerSquareMeter", "milligramsPerSquareMeter", "kilogramsPerHectare", 
   "tonnePerHectare", "poundsPerSquareInch", "kilogramPerCubicMeter", 
   "milliGramsPerMilliLiter", "gramsPerLiter", "milligramsPerCubicMeter", 
   "microgramsPerLiter", "milligramsPerLiter", "gramsPerCubicCentimeter", 
   "gramsPerMilliliter", "gramsPerLiterPerDay", "litersPerSecond", 
   "cubicMetersPerSecond", "cubicFeetPerSecond", "squareMeter", "are", 
   "hectare", "squareKilometers", "squareMillimeters", "squareCentimeters", 
   "acre", "squareFoot", "squareYard", "squareMile", "litersPerSquareMeter", 
   "bushelsPerAcre", "litersPerHectare", "squareMeterPerKilogram", 
   "metersPerSecond", "metersPerDay", "feetPerDay", "feetPerSecond", 
   "feetPerHour", "yardsPerSecond", "milesPerHour", "milesPerSecond", 
   "milesPerMinute", "centimetersPerSecond", "millimetersPerSecond", 
   "centimeterPerYear", "knots", "kilometersPerHour", "metersPerSecondSquared", 
   "waveNumber", "cubicMeterPerKilogram", "cubicMicrometersPerGram", 
   "amperePerSquareMeter", "amperePerMeter", "molePerCubicMeter", "molarity", 
   "molality", "candelaPerSquareMeter", "metersSquaredPerSecond", 
   "metersSquaredPerDay", "feetSquaredPerDay", 
   "kilogramsPerMeterSquaredPerSecond", "gramsPerCentimeterSquaredPerSecond", 
   "gramsPerMeterSquaredPerYear", "gramsPerHectarePerDay", 
   "kilogramsPerHectarePerYear", "kilogramsPerMeterSquaredPerYear", 
   "molesPerKilogram", "molesPerGram", "millimolesPerGram", 
   "molesPerKilogramPerSecond", "nanomolesPerGramPerSecond", 
   "kilogramsPerSecond", "tonnesPerYear", "gramsPerYear", 
   "numberPerMeterSquared", "numberPerKilometerSquared", "numberPerMeterCubed", 
   "numberPerLiter", "numberPerMilliliter", "metersPerGram", "numberPerGram", 
   "gramsPerGram", "microgramsPerGram", "cubicCentimetersPerCubicCentimeters",
   #angleUnitType
   "radian", "degree", "grad"
   ]
