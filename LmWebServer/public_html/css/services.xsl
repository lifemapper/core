<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="2.0"
                xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns:lm="http://lifemapper.org"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns="http://www.w3.org/1999/xhtml">
  <xsl:output
    method="html"
    doctype-system="http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"
    media-type="text/html"
    indent="yes"
    encoding="UTF-8"/>
   
   <xsl:variable name="root" select="/" /> 
   <xsl:template match="/">
      <html xmlns="http://www.w3.org/1999/xhtml">
         <head>
            <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
            <title>
               <xsl:if test="lm:response/lm:title">
                  <xsl:value-of select="lm:response/lm:title" />
               </xsl:if>
            </title>            
            
            <!-- Favicon -->
            <link rel="shortcut icon" href="http://lifemapper.org/wp-content/themes/lifemapper/images/favicon.ico" />
            
            <!--Stylesheets-->
            <link href="http://lifemapper.org/wp-content/themes/lifemapper/services.css"  media="screen" rel="stylesheet" />
            <link href="http://lifemapper.org/wp-content/themes/lifemapper/lifemapper_big.css" type="text/css" media="screen and (min-width: 900px)" rel="stylesheet" />
            <link href="http://lifemapper.org/wp-content/themes/lifemapper/lifemapper_mobile.css"  media="screen and (max-width: 600px)" rel="stylesheet" />
            <link href="http://lifemapper.org/wp-content/themes/lifemapper/style.css"  media="screen and (min-width: 600px) and (max-width: 900px)" rel="stylesheet" />
            <link rel='stylesheet' id='abc_style-css'  href='http://lifemapper.org/wp-content/plugins/advanced-browser-check/css/style.css?ver=3.5.1' type='text/css' media='all' />
            
            <script type='text/javascript' src='http://lifemapper.org/wp-includes/js/jquery/jquery.js?ver=1.8.3'></script>
            <script type='text/javascript' src='http://lifemapper.org/wp-content/plugins/advanced-browser-check/js/jquery.cookie.js?ver=3.5.1'></script>
            <script type='text/javascript' src='http://lifemapper.org/wp-content/plugins/advanced-browser-check/js/script.js?ver=3.5.1'></script>
            
            <link rel="EditURI" type="application/rsd+xml" title="RSD" href="http://lifemapper.org/xmlrpc.php?rsd" />
            <link rel="wlwmanifest" type="application/wlwmanifest+xml" href="http://lifemapper.org/wp-includes/wlwmanifest.xml" /> 
         </head>   
         <body>
            <div id="wrapper">
               <div id="header">
                  <img src="http://lifemapper.org/wp-content/themes/lifemapper/images/world_logo_blue_new.png" />
                  <div id="nav_menu">
                     <ul>
                        <li class="page_item page-item-444"><a href="http://lifemapper.org/?page_id=444">Home</a></li>
                        <li class="page_item page-item-12"><a href="http://lifemapper.org/?page_id=12">Species Distribution</a></li>
                        <li class="page_item page-item-15"><a href="http://lifemapper.org/?page_id=15">Range &#038; Diversity</a></li>
                        <li class="page_item page-item-18"><a href="http://lifemapper.org/?page_id=18">Lifemapper Tools</a></li>
                        <li class="page_item page-item-2"><a href="http://lifemapper.org/?page_id=2">About Lifemapper</a></li>
                     </ul>
                  </div>  <!--#nav_menu -->
                  <form method="get" id="searchform" action="http://lifemapper.org/">
                     <div>
                        <input type="text" size="18" value="" name="s" id="s" />
                        <input type="submit" id="searchsubmit" value="Search" class="btn" />
                     </div>
                  </form>
               </div>  <!--#header -->
               
               <div class="clear"></div>
               
               <div id="services">
                  <xsl:apply-templates />
               </div> <!-- #container -->
               
               <div class="clear"></div>  
               
               <div id="footer">
                  <p>
                     Lifemapper is supported by
                     <img src="/wp-includes/images/NSFlogo_32.png" />
                     US NSF grants EPS-0919443, OCI-1135510, EF-0851290, DEB-1208472
                     <br />and
                     <img src="/wp-includes/images/NASAlogo_32.png" />
                     NASA award ROSES-NNX12AF45A.
                  </p>
               </div> <!-- #wrapper -->
            </div>
         </body>     
      </html>
   </xsl:template>
   
   <xsl:template match="lm:interfaces">
      <span class="interfaces">
         Other Formats:
      </span>
      <ul class="formatsList">
         <xsl:if test="lm:ascii">
            <li class="formatLink">
               <a class="tiffLink" href="{lm:ascii}">ASCII</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:atom">
            <li class="formatLink">
               <a class="atomLink" href="{lm:atom}">ATOM</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:csv">
            <li class="formatLink">
               <a class="csvLink" href="{lm:csv}">CSV</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:eml">
            <li class="formatLink">
               <a class="emlLink" href="{lm:eml}">EML</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:json">
            <li class="formatLink">
               <a class="jsonLink" href="{lm:json}">JSON</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:kml">
            <li class="formatLink">
               <a class="kmlLink" href="{lm:kml}">KML</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:model">
            <li class="formatLink">
               <a class="rawLink" href="{lm:model}">MODEL</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:package">
            <li class="formatLink">
               <a class="packageLink" href="{lm:package}">PACKAGE</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:raw">
            <li class="formatLink">
               <a class="rawLink" href="{lm:raw}">RAW</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:shapefile">
            <li class="formatLink">
               <a class="shapefileLink" href="{lm:shapefile}">SHAPEFILE</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:status">
            <li class="formatLink">
               <a class="statusLink" href="{lm:status}">STATUS</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:tiff">
            <li class="formatLink">
               <a class="tiffLink" href="{lm:tiff}">TIFF</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:wcs">
            <li class="formatLink">
               <a class="wcsLink" href="{lm:wcs}">WCS</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:wms">
            <li class="formatLink">
               <a class="wmsLink" href="{lm:wms}">WMS</a>
            </li>
         </xsl:if>
         <xsl:if test="lm:xml">
            <li class="formatLink">
               <a class="xmlLink" href="{lm:xml}">XML</a>
            </li>
         </xsl:if>
      </ul>
      <br />
      <br />
   </xsl:template>
   
   <xsl:template match="lm:items">
      <xsl:if test="lm:queryParameters">
         <div class="queryFilters" align="center">
            <form method="get" action="#">
               <span class="queryFiltersLabel">
                  Optional Query Filters
               </span>
               <table class="queryFilters">
                  <xsl:for-each select="lm:queryParameters/*">
                     <tr>
                        <td class="queryFilters">
                           <xsl:value-of select="lm:param/lm:displayName" />
                        </td>
                        <xsl:variable name="val" select="normalize-space(lm:value)" />
                        <xsl:choose>
                           <xsl:when test="lm:param/lm:options">
                              <td class="queryFilters">
                                 <select name="{normalize-space(lm:param/lm:name)}">
                                    <option value="">
                                       
                                    </option>
                                    <xsl:for-each select="lm:param/lm:options/lm:option">
                                       <xsl:choose>
                                          <xsl:when test="normalize-space(lm:value) = $val">
                                             <option value="{normalize-space(lm:value)}" selected="selected">
                                                <xsl:value-of select="lm:name" />
                                             </option>
                                          </xsl:when>
                                          <xsl:otherwise>
                                             <option value="{normalize-space(lm:value)}">
                                                <xsl:value-of select="lm:name" />
                                             </option>
                                          </xsl:otherwise>
                                       </xsl:choose>
                                    </xsl:for-each>
                                 </select>
                              </td>
                              <td class="queryFilters">
                                 <xsl:value-of select="normalize-space(lm:param/lm:documentation)" />
                              </td>
                           </xsl:when>
                           <xsl:when test="normalize-space(lm:param/lm:type) = 'date'">
                              <td class="queryFilters">
                                 <input type="text" name="{normalize-space(lm:param/lm:name)}"
                                                    id="{normalize-space(lm:param/lm:name)}" 
                                                    value="{normalize-space(lm:value)}" />
                              </td>
                              <td class="queryFilters">
                                 <xsl:value-of select="normalize-space(lm:param/lm:documentation)" />
                              </td>
                           </xsl:when>
                           <xsl:otherwise>
                              <td class="queryFilters">
                                 <input type="text" name="{normalize-space(lm:param/lm:name)}" 
                                                 value="{normalize-space(lm:value)}" />
                              </td>
                              <td class="queryFilters">
                                 <xsl:value-of select="normalize-space(lm:param/lm:documentation)" />
                              </td>
                           </xsl:otherwise>
                        </xsl:choose>
                     </tr>
                  </xsl:for-each>
               </table>
               <input type="submit" value="Filter Results" />
            </form>
            <br />
            Url That Generated These Results: <br />
            <a href="{$root/lm:response/lm:pages/lm:page[@rel='current']/@href}">
               <xsl:value-of 
                  select="$root/lm:response/lm:pages/lm:page[@rel='current']/@href" />
            </a>
         </div>
      </xsl:if>

      <table class="mainBody">
         <tr>
            <td class="mainBody">
               <xsl:if test="../lm:pages">
                  <table class="pagesTD">
                     <tr>
                        <td class="pagesTD">
                           <span class="pagesHeader">Jump To Page:</span>
                           <ul class="pages">
                              <xsl:for-each select="../lm:pages/lm:page">
                                 <li class="pageLi">
                                    <xsl:if test="./@rel = 'first'">
                                       <a class="pageLink" href="{@href}">
                                          First
                                       </a>
                                    </xsl:if>
                                    <xsl:if test="./@rel = 'previous'">
                                       <a class="pageLink" href="{@href}">
                                          Previous
                                       </a>
                                    </xsl:if>
                                    <xsl:if test="./@rel = 'next'">
                                       <a class="pageLink" href="{@href}">
                                          Next
                                       </a>
                                    </xsl:if>
                                    <xsl:if test="./@rel = 'last'">
                                       <a class="pageLink" href="{@href}">
                                          Last
                                       </a>
                                    </xsl:if>
                                 </li>
                              </xsl:for-each>
                           </ul>
                        </td>
                     </tr>
                  </table>
               </xsl:if>
               <br />
               <span class="pageName">
                  <xsl:value-of select="$root/lm:response/lm:title" />
               </span>
               <table class="items">
                  <tr>
                     <xsl:choose>
                        <xsl:when test="lm:item/lm:modTime">
                           <th class="itemListTh">Id</th>
                           <th class="itemListTh">Title</th>
                           <th class="itemListTh">Modification Time</th>
                        </xsl:when>
                        <xsl:otherwise>
                           <th class="itemListTh">Id</th>
                           <th class="itemListTh">Description</th>
                        </xsl:otherwise>
                     </xsl:choose>
                  </tr>
                  <xsl:for-each select="lm:item">
                     <tr>
                        <xsl:choose>
                           <xsl:when test="lm:modTime">
                              <td class="item">
                                 <a class="link" href="{lm:url}">
                                    <xsl:value-of select="lm:id" />
                                 </a>
                              </td>
                              <td class="item">
                                 <span class="title">
                                    <xsl:value-of select="lm:title" />
                                 </span>
                              </td>
                              <td class="item">
                                 <span class="modTime">
                                    <xsl:value-of select="lm:modTime" />
                                 </span>
                              </td>
                           </xsl:when>
                           <xsl:otherwise>
                              <td class="item">
                                 <a class="link" href="{lm:url}">
                                    <xsl:value-of select="lm:identifier" />
                                 </a>
                              </td>
                              <td class="item">
                                 <span class="title">
                                    <xsl:value-of select="lm:description" />
                                 </span>
                              </td>
                           </xsl:otherwise>
                        </xsl:choose>
                     </tr>
                  </xsl:for-each>
               </table>
            </td>
         </tr>
      </table>
   </xsl:template>
   
   <xsl:template match="lm:countResponse">
      <span class="itemCount">
         Found <xsl:value-of select="lm:count" /> items
      </span>
      <span class="countParametersUsed">
         The following parameters were used:
         <br />
         <xsl:for-each select="lm:parameters/*">
            <span class="countParameter">
               <xsl:value-of select="local-name()" /> = 
               <xsl:value-of select="." />
            </span>
         </xsl:for-each>
      </span>
   </xsl:template>
   
   <xsl:template match="lm:title">
   </xsl:template>
   
   <xsl:template match="lm:user">
   </xsl:template>
   
   <xsl:template match="lm:itemsTitle">
      <span class="itemsTitle"><xsl:value-of select="." /></span>
   </xsl:template>
   
   <xsl:template match="lm:algorithm | lm:algoParamsSet">
      <xsl:if test="lm:code">
         <span class="algorithmCode">
            Algorithm Code: <xsl:value-of select="lm:code" />
         </span>
      </xsl:if>
      <xsl:if test="lm:parameters">
         <span class="algorithmParameters">
            Algorithm Parameters: <br />
         <xsl:if test="lm:parameters/lm:parameters/lm:choice">
            <span class="algorithmParameter">
               Choice:
               <xsl:value-of select="lm:parameters/lm:parameters/lm:choice/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:parameters/lm:coef0">
            <span class="algorithmParameter">
               Coef0 in Kernel Function:
               <xsl:value-of select="lm:parameters/lm:parameters/lm:coef0/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:commissionsamplesize">
            <span class="algorithmParameter">
               Commission Sample Size:
               <xsl:value-of select="lm:parameters/lm:parameters/lm:commissionsamplesize/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:commissionthreshold">
            <span class="algorithmParameter">
               Commission Threshold:
               <xsl:value-of select="lm:parameters/lm:commissionthreshold/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:convergencelimit">
            <span class="algorithmParameter">
               Convergence Limit: 
               <xsl:value-of select="lm:parameters/lm:convergencelimit/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:cost">
            <span class="algorithmParameter">
               Cost:
               <xsl:value-of select="lm:parameters/lm:cost/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:degree">
            <span class="algorithmParameter">
               Degree:
               <xsl:value-of select="lm:parameters/lm:degree/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:distance">
            <span class="algorithmParameter">
               Maximum Distance:
               <xsl:value-of select="lm:parameters/lm:distance/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:distancetype">
            <span class="algorithmParameter">
               Distance Metric (1=Euclidean, 2=Mahalanobis, 
               3=Manhattan/Gower, 4=Chebyshev):
               <xsl:value-of select="lm:parameters/lm:distancetype/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:epoch">
            <span class="algorithmParameter">
               Epoch:
               <xsl:value-of select="lm:parameters/lm:epoch/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:gamma">
            <span class="algorithmParameter">
               Gamma:
               <xsl:value-of select="lm:parameters/lm:gamma/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:gaussianpriorsmoothingcoeficient">
            <span class="algorithmParameter">
               Gaussian Prior Smoothing Coeficient:
               <xsl:value-of 
                       select="lm:parameters/lm:gaussianpriorsmoothingcoeficient/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:hardomissionthreshold">
            <span class="algorithmParameter">
               Hard Omission Threshold:
               <xsl:value-of select="lm:parameters/lm:hardomissionthreshold/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:hiddenlayerneurons">
            <span class="algorithmParameter">
               Hidden Layer Neurons:
               <xsl:value-of select="lm:parameters/lm:hiddenlayerneurons/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:hingefeature">
            <span class="algorithmParameter">
               Hinge Feature:
               <xsl:value-of select="lm:parameters/lm:hingefeature/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:kerneltype">
            <span class="algorithmParameter">
               Kernel Type (0 = linear: u'*v, 1 = polynomial: (gamma*u'*v + 
               coef0)<sup>degree</sup>, 2 = radial basis function: 
               exp(-gamma*|u-v|<sup>2</sup>)):
               <xsl:value-of select="lm:parameters/lm:kerneltype/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:learningrate">
            <span class="algorithmParameter">
               Learning Rate:
               <xsl:value-of select="lm:parameters/lm:learningrate/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:linearfeature">
            <span class="algorithmParameter">
               Linear Feature:
               <xsl:value-of select="lm:parameters/lm:linearfeature/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:maxdist">
            <span class="algorithmParameter">
               Maximum Distance:
               <xsl:value-of select="lm:parameters/lm:maxdist/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:maxgenerations">
            <span class="algorithmParameter">
               Maximum Number of Generations:
               <xsl:value-of select="lm:parameters/lm:maxgenerations/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:maxthreads">
            <span class="algorithmParameter">
               Maximum Nuber of Threads Used: 
               <xsl:value-of select="lm:parameters/lm:maxthreads/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:mincomponents">
            <span class="algorithmParameter">
               Minimum Components:
               <xsl:value-of select="lm:parameters/lm:mincomponents/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:minimumdistance">
            <span class="algorithmParameter">
               Minimum Distance:
               <xsl:value-of select="lm:parameters/lm:minimumdistance/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:minimumerror">
            <span class="algorithmParameter">
               Minimum Error:
               <xsl:value-of select="lm:parameters/lm:minimumerror/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:modelsunderomissionthreshold">
            <span class="algorithmParameter">
               Models Under Omission Threshold: 
               <xsl:value-of 
                           select="lm:parameters/lm:modelsunderomissionthreshold/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:momentum">
            <span class="algorithmParameter">
               Momentum:
               <xsl:value-of select="lm:parameters/lm:momentum/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:nearpointstogetmean">
            <span class="algorithmParameter">
               Nearest 'n' Points to get a Mean:
               <xsl:value-of select="lm:parameters/lm:nearpointstogetmean/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:nu">
            <span class="algorithmParameter">
               Nu (only for Nu-SVC and one-class SVM):
               <xsl:value-of select="lm:parameters/lm:nu/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:numberofiterations">
            <span class="algorithmParameter">
               Number of Iterations:
               <xsl:value-of select="lm:parameters/lm:numberofiterations/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:numberofpseudoabsences">
            <span class="algorithmParameter">
               Number of Pseudo Absences:
               <xsl:value-of select="lm:parameters/lm:numberofpseudoabsences/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:populationsize">
            <span class="algorithmParameter">
               Population Size:
               <xsl:value-of select="lm:parameters/lm:populationsize/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:probabilisticoutput">
            <span class="algorithmParameter">
               Probabilistic Output:
               <xsl:value-of select="lm:parameters/lm:probabilisticoutput/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:productfeature">
            <span class="algorithmParameter">
               Product Feature:
               <xsl:value-of select="lm:parameters/lm:productfeature/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:quadraticfeature">
            <span class="algorithmParameter">
               Quadratic Feature:
               <xsl:value-of select="lm:parameters/lm:quadtraticfeature/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:randomisations">
            <span class="algorithmParameter">
               Randomisations:
               <xsl:value-of select="lm:parameters/lm:randomisations/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:resamples">
            <span class="algorithmParameter">
               Resamples:
               <xsl:value-of select="lm:parameters/lm:resamples/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:standarddeviationcutoff">
            <span class="algorithmParameter">
               Standard Deviation Cutoff:
               <xsl:value-of select="lm:parameters/lm:standarddeviationcutoff/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:standarddeviations">
            <span class="algorithmParameter">
               Standard Deviations:
               <xsl:value-of select="lm:parameters/lm:standarddeviations/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:svmtype">
            <span class="algorithmParameter">
               SVM Type (0 = C-SVC, 1 = Nu-SVM, 2 = one-class SVM):
               <xsl:value-of select="lm:parameters/lm:svmtype/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:terminatetolerance">
            <span class="algorithmParameter">
               Terminate Tolerance:
               <xsl:value-of select="lm:parameters/lm:terminatetolerance/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:thresholdfeature">
            <span class="algorithmParameter">
               Threshold Feature:
               <xsl:value-of select="lm:parameters/lm:thresholdfeature/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:totalruns">
            <span class="algorithmParameter">
               Total Runs: 
               <xsl:value-of select="lm:parameters/lm:totalruns/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:trainingmethod">
            <span class="algorithmParameter">
               Training Method (gis or lbfgs):
               <xsl:value-of select="lm:parameters/lm:trainingmethod/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:trainingproportion">
            <span class="algorithmParameter">
               Training Proportion:
               <xsl:value-of select="lm:parameters/lm:trainingproportion/lm:value" />
            </span>
         </xsl:if>
         
         <xsl:if test="lm:parameters/lm:usedepthrange">
            <span class="algorithmParameter">
               Use Depth Range:
               <xsl:value-of select="lm:parameters/lm:usedepthrange/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:usedistancetoland">
            <span class="algorithmParameter">
               Use Distance To Land:
               <xsl:value-of select="lm:parameters/lm:usedistancetoland/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:useiceconcentration">
            <span class="algorithmParameter">
               Use Ice Concentration:
               <xsl:value-of select="lm:parameters/lm:useiceconcentration/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:useprimaryproduction">
            <span class="algorithmParameter">
               Use Primary Production:
               <xsl:value-of select="lm:parameters/lm:useprimaryproduction/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:usesalinity">
            <span class="algorithmParameter">
               Use Salinity:
               <xsl:value-of select="lm:parameters/lm:usesalinity/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:usesurfacelayers">
            <span class="algorithmParameter">
               Use Surface Layers:
               <xsl:value-of select="lm:parameters/lm:usesurfacelayers/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:usetemperature">
            <span class="algorithmParameter">
               Use Temperature:
               <xsl:value-of select="lm:parameters/lm:usetemperature/lm:value" />
            </span>
         </xsl:if>

         <xsl:if test="lm:parameters/lm:verbosedebugging">
            <span class="algorithmParameter">
               Verbose Debugging (0 = Off, 1 = On):
               <xsl:value-of select="lm:parameters/lm:verbosedebugging/lm:value" />
            </span>
         </xsl:if>
            <xsl:if test="lm:parameters/lm:totalruns">
               <span class="algorithmParameter">
                  Total Runs:
                  <xsl:value-of select="lm:parameters/lm:totalruns/lm:value" />
               </span>
            </xsl:if>
         </span>
      </xsl:if>
   </xsl:template>
   
   <xsl:template match="lm:experiment">
      <xsl:if test="lm:model">
         <span class="expModelHeader">
            Model:
         </span>
         <xsl:if test="lm:model/lm:status = 300">
            <span class="expModelLink">
               <a class="expModelLink" href="{lm:model/lm:metadataUrl}">
                  <xsl:value-of select="lm:model/lm:metadataUrl" />
               </a>
            </span>
         </xsl:if>
         <xsl:if test="lm:model/lm:pointsName">
            <span class="expModelSpeciesName">
               Display Name: <xsl:value-of select="lm:model/lm:pointsName" />
            </span>
         </xsl:if>
         <xsl:if test="lm:model/lm:occurrenceSet/lm:metadataUrl">
            <span class="expModelOccLink">
               Occurrence Set: 
               <a class="expModelOccLink" href="{lm:model/lm:occurrenceSet/lm:metadataUrl}">
                  <xsl:value-of select="lm:model/lm:occurrenceSet/lm:metadataUrl" />
               </a>
            </span>
         </xsl:if>
         <xsl:if test="lm:model/lm:status">
            <span class="expModelStatus">
               Status: 
               <xsl:choose>
                  <xsl:when test="lm:model/lm:status = 1">
                     Initialized
                  </xsl:when>
                  <xsl:when test="lm:model/lm:status = 300">
                     Completed
                  </xsl:when>
                  <xsl:when test="lm:model/lm:status &lt; 300">
                     Running
                  </xsl:when>
                  <xsl:when test="lm:model/lm:status &gt; 300">
                     Error
                  </xsl:when>
                  <xsl:otherwise>
                     Unknown
                  </xsl:otherwise>
               </xsl:choose>
            </span>
         </xsl:if>
         <xsl:if test="lm:model/lm:statusModTime">
            <span class="expModelModTime">
               Last Modified: <xsl:value-of select="lm:model/lm:statusModTime" />
            </span>
         </xsl:if>
         <xsl:apply-templates select="lm:algorithm" />
      </xsl:if>
      
      <xsl:if test="lm:projections">
         <xsl:if test="lm:projections/lm:projection">
            <span class="expProjHeader">
               Projections:
            </span>
         </xsl:if>
         <div align="center">
            <table class="expProjTable">
               <xsl:for-each select="lm:projections/lm:projection">
                  <tr class="expPrjOddRow">
                     <xsl:if test="position() mod 2 = 0">
                        <xsl:attribute name="class">expPrjEvenRow</xsl:attribute>
                     </xsl:if>
                     <td class="expProjLink">
                        <span class="expProjLink">
                           <xsl:if test="lm:status = 300">
                              <a href="{lm:metadataUrl}" class="expProjLink">
                                 <xsl:value-of select="lm:metadataUrl" />
                              </a>
                           </xsl:if>
                        </span>
                        <span class="expProjScnCode">
                           Scenario Code: 
                           <xsl:value-of select="lm:scenarioCode" />
                        </span>
                    </td>
                     <td class="expProjImg">
                        <xsl:choose>
                           <xsl:when test="lm:status = 300">
                              <img src="{lm:metadataUrl}/ogc?height=200&amp;width=400&amp;request=GetMap&amp;service=WMS&amp;bbox={normalize-space(lm:minX)},{normalize-space(lm:minY)},{normalize-space(lm:maxX)},{normalize-space(lm:maxY)}&amp;srs=epsg:{normalize-space(lm:epsgcode)}&amp;format=image/png&amp;color=red&amp;version=1.1.0&amp;layers={normalize-space(lm:mapLayername)}&amp;styles=" alt="WMS Image" class="expProjImg" />
                           </xsl:when>
                           <xsl:otherwise>
                              Status: 
                              <xsl:choose>
                                 <xsl:when test="lm:status = 1">
                                    Initialized
                                 </xsl:when>
                                 <xsl:when test="lm:status = 300">
                                    Completed
                                 </xsl:when>
                                 <xsl:when test="lm:status &lt; 300">
                                    Running
                                 </xsl:when>
                                 <xsl:when test="lm:status &gt; 300">
                                    Error
                                 </xsl:when>
                                 <xsl:otherwise>
                                    Unknown
                                 </xsl:otherwise>
                              </xsl:choose>
                           </xsl:otherwise>
                        </xsl:choose>
                     </td>
                  </tr>
               </xsl:for-each>
            </table>
         </div>
      </xsl:if>
      
      <xsl:if test="lm:bucketList">
         Experiment <xsl:value-of select="lm:id" /> : 
         <xsl:value-of select="lm:name" /><br />
         
         EPSG: <xsl:value-of select="lm:epsgcode" /><br />
         Last modified: <xsl:value-of select="lm:modTime" /><br />
         <br />
         <a href="{normalize-space(lm:metadataUrl)}/buckets" class="expBucketsLink">
            List Buckets
         </a>
         <br />
         <a href="{normalize-space(lm:metadataUrl)}/env" class="expEnvLayersLink">
            List Environment Layers
         </a>
         <br />
         <a href="{normalize-space(lm:metadataUrl)}/pa" class="expOrganismLayersLink">
            List Organism Layers
         </a>
         <br />
         
      </xsl:if>
   </xsl:template>
   
   <xsl:template match="lm:layer">
      <table class="layerServiceTable">
         <tr>
            <td class="layerMetaTD">
               <xsl:if test="lm:title">
                  <span class="layerTitle">
                     Title: <xsl:value-of select="lm:title" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:description">
                  <span class="layerDescription">
                     Description: <xsl:value-of select="lm:description" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:bbox">
                  <span class="layerBbox">
                     Bounding Box: 
                     <xsl:value-of select="lm:minX" />,
                     <xsl:value-of select="lm:minY" />,
                     <xsl:value-of select="lm:maxX" />,
                     <xsl:value-of select="lm:maxY" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:metadataUrl">
                  <span class="layerGuid">
                     Metadata Url:
                     <xsl:value-of select="lm:metadataUrl" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:modTime">
                  <span class="layerModTime">
                     Last Modified:
                     <xsl:value-of select="lm:modTime" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:minVal">
                  <span class="layerMinVal">
                     Minimum Value:
                     <xsl:value-of select="lm:minVal" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:maxVal">
                  <span class="layerMaxVal">
                     Maximum Value:
                     <xsl:value-of select="lm:maxVal" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:nodataVal">
                  <span class="layerNoDataVal">
                     No Data Value:
                     <xsl:value-of select="lm:nodataVal" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:resolution">
                  <span class="layerResolution">
                     Resolution: 
                     <xsl:value-of select="lm:resolution" />
                  </span>
               </xsl:if>
            </td>
            <td class="layerImageTD">
               <xsl:if test="lm:LMModuleType">
                  <img src="{lm:metadataUrl}/ogc?height=200&amp;width=400&amp;request=GetMap&amp;service=WMS&amp;bbox={normalize-space(lm:minX)},{normalize-space(lm:minY)},{normalize-space(lm:maxX)},{normalize-space(lm:maxY)}&amp;srs=epsg:{normalize-space(lm:epsgcode)}&amp;format=image/png&amp;color=red&amp;version=1.1.0&amp;layers={normalize-space(lm:mapLayername)}&amp;styles=" alt="WMS Image" class="layerImage" />
               </xsl:if>
            </td>
         </tr>
      </table>
   </xsl:template>
   
   <xsl:template match="lm:model">
      <xsl:if test="lm:algorithmCode">
         <span class="modelAlgorithmCode">
            Algorithm Code:
            <xsl:value-of select="lm:algorithmCode" />
         </span>
      </xsl:if>
      <xsl:if test="lm:bbox">
         <span class="modelBbox">
            Bounding Box:
            <xsl:value-of select="lm:bbox" />
         </span>
      </xsl:if>
      <xsl:if test="lm:pointsName">
         <span class="modelDisplayName">
            Display Name:
            <xsl:value-of select="lm:pointsName" />
         </span>
      </xsl:if>
      <xsl:if test="lm:scenarioCode">
         <span class="modelScenarioCode">
            Scenario Code:
            <xsl:value-of select="lm:scenarioCode" />
         </span>
      </xsl:if>
      <xsl:if test="lm:status">
         <span class="modelStatus">
            Status:
            <xsl:choose>
               <xsl:when test="lm:status = 1">
                  Initialized
               </xsl:when>
               <xsl:when test="lm:status = 300">
                  Completed
               </xsl:when>
               <xsl:when test="lm:status &lt; 300">
                  Running
               </xsl:when>
               <xsl:when test="lm:status &gt; 300">
                  Error
               </xsl:when>
               <xsl:otherwise>
                  Unknown
               </xsl:otherwise>
            </xsl:choose>
         </span>
      </xsl:if>
      <xsl:if test="lm:statusModTime">
         <span class="modelModTime">
            Last Modified: 
            <xsl:value-of select="lm:statusModTime" />
         </span>
      </xsl:if>
   </xsl:template>
   
   <xsl:template match="lm:occurrence">
      <table class="occSetData">
         <tr>
            <td class="occSetMetaData">
               <xsl:if test="lm:displayName">
                  <span class="occSetDisplayName">
                     Display Name:
                     <xsl:value-of select="lm:displayName" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:bounds">
                  <span class="occSetBbox">
                     Bounding Box:
                     <xsl:value-of select="lm:bounds" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:count">
                  <span class="occSetCount">
                     Count:
                     <xsl:value-of select="lm:queryCount" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:modTime">
                  <span class="occSetModTime">
                     Last Modified:
                     <xsl:value-of select="lm:modTime" />
                  </span>
               </xsl:if>
            </td>
            <td class="occSetImg">
               <xsl:if test="lm:epsgcode = 4326">
                  <img src="{lm:metadataUrl}/ogc?height=200&amp;width=400&amp;request=GetMap&amp;service=WMS&amp;bbox=-180.0,-90.0,180.0,90.0&amp;srs=epsg:{normalize-space(lm:epsgcode)}&amp;format=image/png&amp;color=ffff00&amp;version=1.1.0&amp;layers={normalize-space(lm:mapLayername)}&amp;styles=" alt="WMS Image" class="occSetImg" />
               </xsl:if>
               <xsl:if test="lm:epsgcode = 2163">
                  <img src="{lm:metadataUrl}/ogc?height=200&amp;width=400&amp;request=GetMap&amp;service=WMS&amp;bbox=-4387050,-3732756,4073244,4704460&amp;srs=epsg:{normalize-space(lm:epsgcode)}&amp;format=image/png&amp;color=ffff00&amp;version=1.1.0&amp;layers={normalize-space(lm:mapLayername)}&amp;styles=" alt="WMS Image" class="occSetImg" />
               </xsl:if>
            </td>
         </tr>
      </table>
      <br />
      <xsl:if test="lm:feature">
         <table class="occSetPoints">
            <tr class="occSetPointsHeaderRow">
               <th class="occSetPointsHeader">
                  Catalog Number
               </th>
               <th class="occSetPointsHeader">
                  GBIF Link
               </th>
               <th class="occSetPointsHeader">
                  Longitude
               </th>
               <th class="occSetPointsHeader">
                  Latitude
               </th>
               <th class="occSetPointsHeader">
                  Collection Date
               </th>
               <th class="occSetPointsHeader">
                  Provider Name
               </th>
               <th class="occSetPointsHeader">
                  Resource Name
               </th>
            </tr>
            <xsl:for-each select="lm:feature/lm:feature">
               <tr class="occSetPointsOddRow">
                  <xsl:if test="position() mod 2 = 0">
                     <xsl:attribute name="class">occSetPointsEvenRow</xsl:attribute>
                  </xsl:if>
                  <td class="occSetPointsData">
                     <xsl:choose>
                        <xsl:when test="@catnum">
                           <xsl:value-of select="@catnum" />
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                     <xsl:choose>
                        <xsl:when test="@localid">
                           <xsl:value-of select="@localid" />
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                  </td>
                  <td class="occSetPointsData">
                     <xsl:choose>
                        <xsl:when test="@url">
                           <a href="{@url}" target="_blank">
                              Link
                           </a>
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                  </td>
                  <td class="occSetPointsData">
                     <xsl:choose>
                        <xsl:when test="@longitude">
                           <xsl:value-of select="@longitude" />
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                     <xsl:choose>
                        <xsl:when test="@lon">
                           <xsl:value-of select="@lon" />
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                  </td>
                  <td class="occSetPointsData">
                     <xsl:choose>
                        <xsl:when test="@latitude">
                           <xsl:value-of select="@latitude" />
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                     <xsl:choose>
                        <xsl:when test="@lat">
                           <xsl:value-of select="@lat" />
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                  </td>
                  <td class="occSetPointsData">
                     <xsl:choose>
                        <xsl:when test="@colldate">
                           <xsl:value-of select="substring-before(@colldate, '00:')" />
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                  </td>
                  <td class="occSetPointsData">
                     <xsl:choose>
                        <xsl:when test="@provname">
                           <xsl:value-of select="@provname" />
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                  </td>
                  <td class="occSetPointsData">
                     <xsl:choose>
                        <xsl:when test="@resname">
                           <xsl:value-of select="@resname" />
                        </xsl:when>
                        <xsl:otherwise>
                           
                        </xsl:otherwise>
                     </xsl:choose>
                  </td>
               </tr>
            </xsl:for-each>
         </table>
      </xsl:if>
   </xsl:template>
   
   <xsl:template match="lm:projection">
      <table class="projectionTable">
         <tr>
            <td class="projData">
               <xsl:if test="lm:speciesName">
                  <span class="projDisplayName">
                     Display Name:
                     <xsl:value-of select="lm:speciesName" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:scenarioCode">
                  <span class="projScenarioCode">
                     Scenario Code:
                     <xsl:value-of select="lm:scenarioCode" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:bbox">
                  <span class="projBbox">
                     Bounding Box: 
                     <xsl:value-of select="lm:minX" />,
                     <xsl:value-of select="lm:minY" />,
                     <xsl:value-of select="lm:maxX" />,
                     <xsl:value-of select="lm:maxY" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:metadataUrl">
                  <span class="projGuid">
                     Metadata Url: 
                     <xsl:value-of select="lm:metadataUrl" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:resolution">
                  <span class="projResolution">
                     Resolution:
                     <xsl:value-of select="lm:resolution" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:units">
                  <span class="projUnits">
                     Units
                     <xsl:value-of select="lm:units" />
                  </span>
               </xsl:if>
               <xsl:if test="lm:status">
                  <span class="projStatus">
                     Status:
                     <xsl:choose>
                        <xsl:when test="lm:status = 1">
                           Initialized
                        </xsl:when>
                        <xsl:when test="lm:status = 300">
                           Completed
                        </xsl:when>
                        <xsl:when test="lm:status &lt; 300">
                           Running
                        </xsl:when>
                        <xsl:when test="lm:status &gt; 300">
                           Error
                        </xsl:when>
                        <xsl:otherwise>
                           Unknown
                        </xsl:otherwise>
                     </xsl:choose>
                  </span>
               </xsl:if>
               <xsl:if test="lm:statusModTime">
                  <span class="projModTime">
                     Last Modified:
                     <xsl:value-of select="lm:statusModTime" />
                  </span>
               </xsl:if>
            </td>
            <td class="projImg">
               <img src="{lm:metadataUrl}/ogc?height=200&amp;width=400&amp;request=GetMap&amp;service=WMS&amp;bbox={normalize-space(lm:minX)},{normalize-space(lm:minY)},{normalize-space(lm:maxX)},{normalize-space(lm:maxY)}&amp;srs=epsg:{normalize-space(lm:epsgcode)}&amp;format=image/png&amp;color=red&amp;version=1.1.0&amp;layers={normalize-space(lm:mapLayername)}&amp;styles=" alt="WMS Image" class="projImg" />
            </td>
         </tr>
      </table>
   </xsl:template>
   
   <xsl:template match="lm:scenario">
      <xsl:if test="lm:title">
         <span class="scenTitle">
            Title: <xsl:value-of select="lm:title" />
         </span>
      </xsl:if>
      <xsl:if test="lm:code">
         <span class="scenScenarioCode">
            Scenario Code:
            <xsl:value-of select="lm:code" />
         </span>
      </xsl:if>
      <xsl:if test="lm:bbox">
         <span class="scenBbox">
            Bounding Box:
            <xsl:value-of select="lm:minX" />,
            <xsl:value-of select="lm:minY" />,
            <xsl:value-of select="lm:maxX" />,
            <xsl:value-of select="lm:maxY" />
         </span>
      </xsl:if>
      <xsl:if test="lm:metadataUrl">
         <span class="scenGuid">
            Metadata Url:
            <xsl:value-of select="lm:metadataUrl" />
         </span>
      </xsl:if>
      <xsl:if test="lm:description">
         <span class="description">
            Scenario Description:
            <xsl:value-of select="lm:description" />
         </span>
      </xsl:if>
      <xsl:if test="lm:startDate">
         <span class="scenStartDate">
            Start Date:
            <xsl:value-of select="lm:startDate" />
         </span>
      </xsl:if>
      <xsl:if test="lm:endDate">
         <span class="scenEndDate">
            End Date:
            <xsl:value-of select="lm:endDate" />
         </span>
      </xsl:if>
      <xsl:if test="lm:resolution">
         <span class="scenResolution">
            Resolution:
            <xsl:value-of select="lm:resolution" />
         </span>
      </xsl:if>
      <xsl:if test="lm:units">
         <span class="scenUnits">
            Units:
            <xsl:value-of select="lm:units" />
         </span>
      </xsl:if>
      <xsl:if test="lm:modTime">
         <span class="scenLastModified">
            Last Modified:
            <xsl:value-of select="modTime" />
         </span>
      </xsl:if>
      <xsl:if test="lm:keywords">
         <span class="scenKeywords">
            Keywords:<br />
            <ul class="scenKeywords">
               <xsl:for-each select="lm:keywords/lm:keyword">
                  <li class="scenKeyword">
                     <xsl:value-of select="." />
                  </li>
               </xsl:for-each>
            </ul>
         </span>
      </xsl:if>
      <xsl:if test="lm:layers">
         <span class="scenLayers">
            Layers:<br />
            <ul class="scenLayers">
               <xsl:for-each select="lm:layers/lm:layer">
                  <li class="scenLayer">
                     <a href="{lm:metadataUrl}" class="scenLayer">
                        <xsl:value-of select="lm:metadataUrl" />
                     </a>
                  </li>
               </xsl:for-each>
            </ul>
         </span>
      </xsl:if>
   </xsl:template>
   <xsl:template match="lm:error">
      <span class="errorHeader">
         Sorry, an error occurred:
      </span>
      <xsl:if test="lm:error">
         <span class="errorMessage">
            <xsl:value-of select="lm:error" />
         </span>
      </xsl:if>
   </xsl:template>
   
   <xsl:template match="lm:services">
      <xsl:for-each select="lm:service">
         <span class="serviceUrl">
            Service:
            <a class="serviceUrl" href="{lm:url}">
               <xsl:value-of select="lm:name" />
            </a>
         </span>
         <span class="serviceDescription">
            <xsl:value-of select="lm:description" />
         </span>
         <span class="serviceHelpLink">
            <a class="serviceHelpUrl" href="{lm:helpLink}">
               Get help with the <xsl:value-of select="lm:name" /> service.
            </a>
         </span>
      </xsl:for-each>
   </xsl:template>
   
   <xsl:template match="lm:bucket">
      Bucket <xsl:value-of select="lm:id" /> - <xsl:value-of select="lm:name" /><br />
      <br />
      EPSG Code: <xsl:value-of select="lm:epsgcode" /><br />
      Modified: <xsl:value-of select="lm:modTime" /><br />
      Stage: <xsl:value-of select="lm:stage" />, Status: <xsl:value-of select="lm:status" /><br />
      <br />
      Shapegrid:
      <br />
      Bounding Box: <xsl:value-of select="lm:shapegrid/lm:bbox" /><br />
      Cell Sides: <xsl:value-of select="lm:shapegrid/lm:cellsides" /><br />
      Cell Size: <xsl:value-of select="lm:shapegrid/lm:cellsize" />
                 <xsl:value-of select="lm:shapegrid/lm:mapUnits" /><br />
      Link: <a href="{normalize-space(lm:shapegrid/lm:metadataUrl)}">
               Layer <xsl:value-of select="lm:shapegrid/lm:id" />
            </a>
      <br />
      <br /><br />
      PamSums:<br /><br />
      <a href="{normalize-space(lm:metadataUrl)}/pamsums/original">
         Original PamSum
      </a><br />
      <a href="{normalize-space(lm:metadataUrl)}/pamsums/">
         Random PamSums
      </a><br />
   </xsl:template>

   <xsl:template match="lm:pamsum">
      PamSum <xsl:value-of select="lm:id" /><br />
      <br />
      Modified: <xsl:value-of select="lm:modTime" /><br />
      Stage: <xsl:value-of select="lm:stage" />, Status: <xsl:value-of select="lm:status" /><br />
      
   </xsl:template>
   
</xsl:stylesheet>
