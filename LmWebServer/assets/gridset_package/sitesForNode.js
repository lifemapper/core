/*
Copyright (C) 2017, University of Kansas Center for Research

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
*/

"use strict";

app.ports.requestSitesForNode.subscribe(function(nodeId) {
    const node = nodeLookup.find(function(d) {
        return d.header == nodeId || d.header.toLowerCase() == ("node_" + nodeId);
    });
    const dataColumn = node && node.index;

    const sites = ancPam.features.filter(function(feature) {
        const data = feature.properties.data;
        return (data[1] && data[1].includes(dataColumn))
            ||  (data[-1] && data[-1].includes(dataColumn))
            ||  (data[2] && data[2].includes(dataColumn));
    }).map(function(feature) { return feature.id; });

    app.ports.sitesForNode.send(sites);
});

app.ports.requestNodesForSites.subscribe(function(sites) {
    const leftNodes = new Set();
    const rightNodes = new Set();
    ancPam.features.forEach(function(feature) {
        const data = feature.properties.data;
        if(sites.includes(feature.id)) {
            data[1] && data[1].forEach(function(i) {
                const node = nodeLookup.find(function(d) {
                    return d.index == i;
                });
                leftNodes.add(parseInt(node.header.replace(/^Node_/i, '')));
            });
            data[-1] && data[-1].forEach(function(i) {
                const node = nodeLookup.find(function(d) {
                    return d.index == i;
                });
                rightNodes.add(parseInt(node.header.replace(/^Node_/i, '')));
            });
        }
    });
    const nodesForSites = [Array.from(leftNodes), Array.from(rightNodes)];
    app.ports.nodesForSites.send(nodesForSites);
});
