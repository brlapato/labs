function writeLakekeeperIcebergTable(T, namespace, tableName, options)
%WRITELAKEKEEPERICEBERGTABLE Write a MATLAB table to an Apache Iceberg
%table managed by a Lakekeeper REST catalog.
%
%   writeLakekeeperIcebergTable(T, namespace, tableName) writes table T
%   to the Iceberg table NAMESPACE.TABLENAME via a Lakekeeper REST
%   catalog, using default connection options below.
%
%   writeLakekeeperIcebergTable(..., 'Name', Value, ...) accepts:
%       CatalogUri     - Lakekeeper REST catalog endpoint
%                        (default "http://localhost:8181/catalog")
%       Warehouse      - warehouse name registered in Lakekeeper
%                        (default "warehouse")
%       ClientId       - OAuth2 client id (leave empty for no auth)
%       ClientSecret   - OAuth2 client secret
%       Scope          - OAuth2 scope (default "lakekeeper")
%       Mode           - 'append' (default), 'overwrite', or 'create'
%       S3Endpoint     - S3-compatible endpoint URL, e.g.
%                        "http://localhost:9000" for a local MinIO
%                        instance (leave empty to use AWS default)
%       S3AccessKeyId  - access key for the S3-compatible store
%       S3SecretAccessKey - secret key for the S3-compatible store
%       S3Region       - region string; some local S3 stores require
%                        any non-empty value even if unused
%                        (default "us-east-1")
%       S3PathStyleAccess - true (default) to address buckets as
%                        endpoint/bucket/key instead of
%                        bucket.endpoint/key. Required by most local
%                        S3-compatible stores (e.g. MinIO).
%
%   EXAMPLE
%       T = table((1:5)', {'a';'b';'c';'d';'e'}, 'VariableNames', {'id','label'});
%       writeLakekeeperIcebergTable(T, "sales", "orders", ...
%           "CatalogUri", "https://my-lakekeeper.example.com/catalog", ...
%           "Warehouse", "my_warehouse", ...
%           "ClientId", "my-client-id", ...
%           "ClientSecret", "my-client-secret", ...
%           "Mode", "append");
%
%   REQUIREMENTS
%       - MATLAB's Python interface configured (pyenv) pointing to a
%         Python environment with pyiceberg + pyarrow installed:
%             pip install "pyiceberg[pyarrow]"
%       - Network access to the Lakekeeper REST catalog endpoint.
%
%   NOTES
%       Iceberg's on-disk metadata (manifests, snapshots, table
%       metadata JSON) has no native MATLAB implementation, so this
%       function writes T to a temporary Parquet file with MATLAB's
%       built-in parquetwrite, then delegates the actual Iceberg
%       commit (schema registration, snapshot creation, catalog
%       update) to Python's pyiceberg library through MATLAB's py.
%       interface.

arguments
    T table
    namespace (1,:) char
    tableName (1,:) char
    options.CatalogUri (1,:) char = "http://localhost:8181/catalog"
    options.Warehouse (1,:) char = "warehouse"
    options.ClientId (1,:) char = ""
    options.ClientSecret (1,:) char = ""
    options.Scope (1,:) char = "lakekeeper"
    options.Mode (1,:) char {mustBeMember(options.Mode, {'append','overwrite','create'})} = "append"
    options.S3Endpoint (1,:) char = ""
    options.S3AccessKeyId (1,:) char = ""
    options.S3SecretAccessKey (1,:) char = ""
    options.S3Region (1,:) char = "us-east-1"
    options.S3PathStyleAccess (1,1) logical = true
end

    % --- 1. Verify Python environment & pyiceberg availability ---
    pe = pyenv;
    if pe.Status == "NotLoaded"
        error("writeLakekeeperIcebergTable:PythonNotConfigured", ...
            "No Python environment loaded. Run pyenv('Version','/path/to/python') first.");
    end
    try
        py.importlib.import_module('pyiceberg');
    catch
        error("writeLakekeeperIcebergTable:MissingPyIceberg", ...
            "pyiceberg is not installed in the active Python environment.\n" + ...
            "Install with: pip install ""pyiceberg[pyarrow]""");
    end

    % --- 2. Write the MATLAB table to a temporary Parquet file ---
    tmpDir = tempname;
    mkdir(tmpDir);
    cleanupObj = onCleanup(@() rmdir(tmpDir, 's')); %#ok<NASGU>
    parquetFile = fullfile(tmpDir, "data.parquet");
    parquetwrite(parquetFile, T);

    % --- 3. Connect to the Lakekeeper REST catalog ---
    pyicb = py.importlib.import_module('pyiceberg.catalog');
    pq = py.importlib.import_module('pyarrow.parquet');

    catalogArgs = {'type', 'rest', 'uri', options.CatalogUri, 'warehouse', options.Warehouse};

    if strlength(options.ClientId) > 0
        catalogArgs = [catalogArgs, {'credential', sprintf('%s:%s', options.ClientId, options.ClientSecret), ...
            'scope', options.Scope}];
    end

    % S3-compatible storage (e.g. local MinIO) config, forwarded by
    % pyiceberg to its PyArrowFileIO / S3FileSystem.
    if strlength(options.S3Endpoint) > 0
        catalogArgs = [catalogArgs, {'s3.endpoint', options.S3Endpoint}];
    end
    if strlength(options.S3AccessKeyId) > 0
        catalogArgs = [catalogArgs, {'s3.access-key-id', options.S3AccessKeyId, ...
            's3.secret-access-key', options.S3SecretAccessKey}];
    end
    if strlength(options.S3Region) > 0
        catalogArgs = [catalogArgs, {'s3.region', options.S3Region}];
    end
    if options.S3PathStyleAccess
        catalogArgs = [catalogArgs, {'s3.path-style-access', 'true'}];
    else
        catalogArgs = [catalogArgs, {'s3.path-style-access', 'false'}];
    end

    catalog = pyicb.load_catalog('lakekeeper', pyargs(catalogArgs{:}));

    % --- 4. Read the Parquet file back as a pyarrow Table (gives schema) ---
    arrowTable = pq.read_table(parquetFile);

    % --- 5. Ensure namespace exists ---
    nsParts = strsplit(namespace, '.');
    nsTuple = py.tuple(nsParts);
    try
        catalog.create_namespace(nsTuple);
    catch
        % Namespace already exists - safe to ignore
    end

    % --- 6. Load or create the table ---
    fullIdent = py.tuple({namespace, tableName});
    tableExists = logical(catalog.table_exists(fullIdent));

    if strcmp(options.Mode, 'create') && tableExists
        error("writeLakekeeperIcebergTable:TableExists", ...
            "Table %s.%s already exists; use Mode 'append' or 'overwrite' instead.", ...
            namespace, tableName);
    end

    if ~tableExists
        icebergTable = catalog.create_table(pyargs( ...
            'identifier', fullIdent, ...
            'schema', arrowTable.schema));
    else
        icebergTable = catalog.load_table(fullIdent);
    end

    % --- 7. Commit the data ---
    switch options.Mode
        case "overwrite"
            icebergTable.overwrite(arrowTable);
        otherwise  % "append" or "create"
            icebergTable.append(arrowTable);
    end

    fprintf("Wrote %d rows to Iceberg table %s.%s via Lakekeeper (mode=%s)\n", ...
        height(T), namespace, tableName, options.Mode);

end
