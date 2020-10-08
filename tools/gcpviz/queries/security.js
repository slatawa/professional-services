var containerResourceTypes = [
    "cloudresourcemanager.googleapis.com/Organization",
    "cloudresourcemanager.googleapis.com/Folder",
    "cloudresourcemanager.googleapis.com/Project",
];
var wantedResourceTypes = [
    "cloudkms.googleapis.com/KeyRing",
    "cloudkms.googleapis.com/CryptoKey",
    "dns.googleapis.com/ManagedZone",
    "dns.googleapis.com/Policy",
    "compute.googleapis.com/Network",
    "compute.googleapis.com/Firewall",
    "compute.googleapis.com/PacketMirroring",
    "compute.googleapis.com/SecurityPolicy",
    "compute.googleapis.com/SslCertificate",
    "compute.googleapis.com/SslPolicy",
    "logging.googleapis.com/LogMetric",
    "logging.googleapis.com/LogSink",
];
var resourceTypes = containerResourceTypes.concat(wantedResourceTypes);

var nodes = [];
var follow = function (n, depth) {
    var out = n.tag("parent").labelContext(resourceTypes, "type").out("child");
    if (out.count() == 0) {
        return;
    }
    nodes = nodes.concat(out.tagArray());
    follow(out, depth + 1);
};

// Filters disconnected vertexes from results
var filterEmptyNodes = function (nodes) {
    var filteredNodes = [];
    var m = g.Morphism().labelContext(resourceTypes, "type").in(["child", "uses"]);
    nodes.forEach(function (node) {
        if (wantedResourceTypes.indexOf(node.type) > -1) {
            if (g.V(node.id).follow(m).count() > 0) {
                filteredNodes.push(node);
            }
        } else {
            filteredNodes.push(node);
        }
    });
    return filteredNodes;
}

// Filters empty projects from results
var filterEmptyProjects = function (nodes) {
    var filteredNodes = [];
    var projectM = g.Morphism().labelContext(resourceTypes, "type").in(["child", "uses"]);
    nodes.forEach(function (node) {
        if (node.type == "cloudresourcemanager.googleapis.com/Project") {
            if (g.V(node.id).follow(projectM).count() > 1) {
                filteredNodes.push(node);
            }
        } else {
            filteredNodes.push(node);
        }
    });
    return filteredNodes;
}

// Filters empty folders from results
var filterEmptyFolders = function (nodes) {
    var folderMap = {};
    var folderItemCount = {};
    var filteredNodes = [];

    nodes.forEach(function (node) {
        if (containerResourceTypes.indexOf(node.type) > -1) {
            folderMap[node.id] = node;
            if (node.type == "cloudresourcemanager.googleapis.com/Folder") {
                folderItemCount[node.id] = 0;
            }
        }
    });

    nodes.forEach(function (node) {
        if (node.type == "cloudresourcemanager.googleapis.com/Project") {
            var iNode = node;
            while (iNode && iNode.parent in folderMap) {
                folderItemCount[iNode.parent] += 1;
                iNode = folderMap[iNode.parent];
            }
        }
    });

    nodes.forEach(function (node) {
        if (node.type == "cloudresourcemanager.googleapis.com/Folder") {
            if (folderItemCount[node.id] > 0) {
                filteredNodes.push(node);
            }
        } else {
            filteredNodes.push(node);
        }
    });
    return filteredNodes;
}

var root = g.V("{{ index .Organizations 0 }}");
follow(root, 1);
filterEmptyFolders(filterEmptyProjects(root.tagArray().concat(nodes))).forEach(function (node) {
    g.emit(node);
});