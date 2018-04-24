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

var maps = {};
var mapLayers = {};

// document.onmousemove = document.onmouseup = document.onmousedown = function(event) {
//     const plot = document.getElementById("plot");
//     if (plot == null) return;
//     const rect = plot.getBoundingClientRect();
//     const x = event.clientX - rect.left;
//     const y = event.clientY - rect.top;
//     app.ports.mouseEvent.send([event.type, x, y]);
// };

function configureMap(element) {
    var map = maps[element._leaflet_id];
    if (map == null) return;
    console.log("updating leaflet id", element._leaflet_id);

    var layers = mapLayers[element._leaflet_id];
    if (layers != null) {
        layers.forEach(function(layer) {  map.removeLayer(layer); });
    }

    const pavs = element.dataset["mapPavs"].split("\n").map(function(pav) {
        return pav.split(" ");
    });

    const pavStates = pavs.map(function(pav) {
        const header = pav[0];
        return {index: 1, runlength: parseInt(pav[1]), value: header === "v1s1" ? 1 : 0};
    });

    let maxCount = 0;
    const counts = Array(shapeGrid.features.length).fill(0);
    for (let i = 0; i < shapeGrid.features.length; i++) {
        for (let j = 0; j < pavStates.length; j++) {
            const state = pavStates[j];
            counts[i] += state.value;
            maxCount = (counts[i] > maxCount) ? counts[i] : maxCount;
            state.runlength--;
            if (state.runlength < 1) {
                state.index++;
                state.runlength = parseInt(pavs[j][state.index]);
                state.value = state.value === 1 ? 0 : 1;
            }
        }
    }


    console.log("adding layer");

    // if (layers == null || layers.length === 0) {
        mapLayers[element._leaflet_id] = [
            L.geoJSON(shapeGrid, {
                style: style(counts, maxCount),
                filter: function(feature) {
                    return counts[parseInt(feature.properties.siteid)] > 0;
                }
            }).addTo(map)
        ];
    // } else {
    //     layers[0].setStyle(style(dataColumn));
    // }
}

function style(counts, maxCount) {
    return function(feature) {
        const siteId = parseInt(feature.properties.siteid);

        return {
            fillOpacity: counts[siteId] / maxCount,
            stroke: false,
            fill: true,
            fillColor: "red"
        };
    };
}


var observer = new MutationObserver(function(mutations) {
    mutations.forEach(function(m) {
        m.addedNodes.forEach(function(n) {
            if (n.getElementsByClassName == null) return;

            var elements = n.getElementsByClassName("leaflet-map");
            Array.prototype.forEach.call(elements, function(element) {
                var map = L.map(element, {center: [0,0], zoom: 2});
                L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    minZoom: 1,
                    maxZoom: 12
                }).addTo(map);
                maps[element._leaflet_id] = map;
                console.log("added leaflet id", element._leaflet_id);
                configureMap(element);
            });
        });

        m.removedNodes.forEach(function(n) {
            if (n.getElementsByClassName == null) return;

            var elements = n.getElementsByClassName("leaflet-map");
            Array.prototype.forEach.call(elements, function(element) {
                if (element._leaflet_id != null) {
                    console.log("removing map with leaflet id", element._leaflet_id);
                    maps[element._leaflet_id].remove();
                    maps[element._leaflet_id] = null;
                    mapLayers[element._leaflet_id] = null;
                }
            });
        });

        if (m.type == "attributes") {
            configureMap(m.target);
        }
    });
});

observer.observe(document.body, {
    subtree: true,
    childList: true,
    attributes: true,
    attributeFilter: ["data-map-pavs"],
    attributeOldValue: true
});

