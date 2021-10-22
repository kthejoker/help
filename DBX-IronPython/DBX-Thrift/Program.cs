using System;
using Thrift.Transport.Client;

//using Microsoft.Practices.ServiceLocation;
using Thrift.Protocol;

using Thrift.Transport;
using Thrift;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Security.Cryptography.X509Certificates;
//using System.Linq;

namespace DBX_Thrift
{


public class Connection {

    public THttpTransport _transport {get; set;}

    public TCLI.TCLIService.Client _client {get; set;}

    public TCLI.TSessionHandle _sessionHandle {get; set;}

    public Cursor getCursor() {
        return new Cursor(this);
    }

    public Connection(string host, string httpPath, string token) {

        int port = 443;
       Uri server =  new Uri($"https://{host}:{port}/{httpPath}");


        /*

    ssl_context = create_default_context(cafile=kwargs.get("_tls_trusted_ca_file"))
        if kwargs.get("_tls_no_verify") is True:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_NONE
        elif kwargs.get("_tls_verify_hostname") is False:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_REQUIRED
        else:
            ssl_context.check_hostname = True
            ssl_context.verify_mode = CERT_REQUIRED

        tls_client_cert_file = kwargs.get("_tls_client_cert_file")
        tls_client_cert_key_file = kwargs.get("__tls_client_cert_key_file")
        tls_client_cert_key_password = kwargs.get("_tls_client_cert_key_password")
        if tls_client_cert_file:
            ssl_context.load_cert_chain(
                certfile=tls_client_cert_file,
                keyfile=tls_client_cert_key_file,
                password=tls_client_cert_key_password
            )


        */

      
        //_transport = new TSocketTransport(URI, 443, null); //SSL

        Dictionary<string, string> opts =
    new Dictionary<string, string>();

     string user_agent_header = "PyDatabricksSqlConnector/0.9.1";


    opts.Add("Authorization", $"Bearer {token}");
    opts.Add("Host", host);

        _transport = new THttpTransport(server, null, opts, user_agent_header);
        TProtocol protocol = new TBinaryProtocol(_transport);
        _client = new TCLI.TCLIService.Client(protocol);

        

        
    }

     public async Task openSession() {
CancellationTokenSource ctSource = new CancellationTokenSource();
        CancellationToken cToken = ctSource.Token;
        TCLI.TOpenSessionResp r = new TCLI.TOpenSessionResp();
var pVersion = TCLI.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;
            try {
            await _transport.OpenAsync(cToken);
            TCLI.TOpenSessionReq open_session_req = new TCLI.TOpenSessionReq(pVersion);
            var response = _client.OpenSession(open_session_req);
            
             r = await response;
            _sessionHandle = r.SessionHandle;
        }
        catch (Exception ex) {
            Console.WriteLine(r);
            _transport.Close();
            Console.WriteLine(ex.Message);
        }

        }
       
    }
        

public class Cursor {

    public const int _STATE_NONE = 0;
    public const int _STATE_RUNNING = 1;
    public const int _STATE_FINISHED = 2;

    public int _state {get;set ;}

    public Connection _conn {get; set;}

    public TCLI.TOperationHandle _operationHandle {get; set;}

    public long _arraySize { get;set; }

    public Dictionary<string,string> _description { get; set;}

    public Cursor(Connection c) {
        _conn = c;

    }

    public async Task<Dictionary<string, string>> setDescription() {
        if (_operationHandle == null) { return _description;}
        if (_description != null) { return _description;}
        var req = new TCLI.TGetResultSetMetadataReq(_operationHandle);
        TCLI.TGetResultSetMetadataResp response = await _conn._client.GetResultSetMetadata(req);
        List<TCLI.TColumnDesc> columns = response.Schema.Columns;
        columns.ForEach(delegate(TCLI.TColumnDesc col) {
                string typeCode = null;
      
                var primaryTypeEntry = col.TypeDesc.Types[0];
                if (primaryTypeEntry.PrimitiveEntry == null) {
                    typeCode = "STRING";
                }
                else {
                    typeCode = Enum.GetName(typeof(TCLI.TTypeId), primaryTypeEntry.PrimitiveEntry.Type).ToString();
                   // typeCode = TCLI.TTypeId.
                }
                if (typeCode.EndsWith("_TYPE")) {
                        typeCode = typeCode.Substring(typeCode.Length - 5);
                }  
                typeCode.ToLower();
                _description.Add(col.ColumnName, typeCode);

        });

            
                return _description;
    }

   
    public void ResetState() {
        _description.Clear();
        //if self._operationHandle is not None:
         //   request = ttypes.TCloseOperationReq(self._operationHandle)
        ////    try:
         //       response = self._connection._client.CloseOperation(request)
        //        _check_status(response)
        //    finally:
            _operationHandle = null;
    }

    public Cursor enter() {
        return this;
    }

    public void exit() {
     this.close();
    }

    public void close() {
        this.ResetState();
    }

    public async Task execute(string operation) {
        var req = new TCLI.TExecuteStatementReq(_conn._sessionHandle, operation);
        TCLI.TExecuteStatementResp response = await _conn._client.ExecuteStatement(req);
        _operationHandle = response.OperationHandle;

    }

    public async Task<TCLI.TGetOperationStatusResp> Poll(bool getProgressUpdate = true) {
        if (_state == _STATE_NONE) {
            throw new Exception("No query yet");
        }

       var req = new TCLI.TGetOperationStatusReq(_operationHandle);
       var response = _conn._client.GetOperationStatus(req);
       return await response;
    }

    public async Task fetchAll() {
        var req = new TCLI.TFetchResultsReq(_operationHandle, TCLI.TFetchOrientation.FETCH_NEXT, _arraySize);
        TCLI.TFetchResultsResp r = await _conn._client.FetchResults(req);
        Dictionary<string, string> schema = await setDescription();
       
       var zipped = schema.Zip(r.Results.Columns, (a, b) => new { a, b});
       zipped.
        Console.WriteLine(r.Results.ColumnCount);

    }

}


    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            string serverHostName = "adb-5206439413157315.15.azuredatabricks.net";
             string httpPath = "sql/1.0/endpoints/b891039e995229bc";
            string token = "dapi9380e6788b4ac346cd6962a9dede7106";
            

Connection c = new Connection(serverHostName, httpPath, token);
             await c.openSession();
Cursor x = c.getCursor();
x.execute("select 1 as A, 2 as B").Wait();
x.fetchAll().Wait();

  

        }
    }

}