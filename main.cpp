#include <mongo/client/dbclient.h>

#include <iostream>
#include <fstream>
#include <memory>
#include <limits>
#include <exception>

#include <arpa/inet.h>

using namespace std;
using namespace mongo;

ofstream& writeInt(ofstream& out, long long v)
{
    if (v > numeric_limits<int>::max())
        throw runtime_error("toooooo looooong!");
    int nv = htonl(static_cast<int>(v));
    out.write(reinterpret_cast<char*>(&nv), sizeof(nv));
    return out;
}

int run(DBClientBase* conn) {
    auto_ptr<DBClientCursor> cursor = conn->query("vkuu.friends_list", BSONObj());

    if (!cursor.get()) {
        cout << "query failure" << endl;
        return EXIT_FAILURE;
    }

    ofstream fout("friends.bin", ios::binary);

    for (int i=0; cursor->more(); ++i ) {
        BSONObj r = cursor->next();

        long long uid = r.getField("uid").numberLong();
        BSONObj friends = r.getObjectField("friends");

        if (friends.nFields()>0) {
            writeInt(fout, uid);
            BSONObjIterator frIter (friends);

            while (frIter.more()) {
                long long fr = frIter.next().numberLong();
                writeInt(fout, fr);
            }

            writeInt(fout, 0);
        }

        if (i%20000==0)
            cout << "passed: " << i << endl;
    }

    cout << cnt << endl;

    return EXIT_SUCCESS;
}

int main()
{
    client::GlobalInstance instance;
    if (!instance.initialized()) {
        std::cout << "failed to initialize the client driver: " << instance.status() << std::endl;
        return EXIT_FAILURE;
    }

    const string uri = "mongodb://localhost:27017";
    string errmsg;

    ConnectionString cs = ConnectionString::parse(uri, errmsg);

    if (!cs.isValid()) {
        cout << "Error parsing connection string " << uri << ": " << errmsg << endl;
        return EXIT_FAILURE;
    }

    unique_ptr<DBClientBase> conn(cs.connect(errmsg));
    if ( !conn ) {
        cout << "couldn't connect : " << errmsg << endl;
        return EXIT_FAILURE;
    }

    int ret = EXIT_SUCCESS;
    try {
        ret = run(conn.get());
    }
    catch( exception &e ) {
        cout << "caught " << e.what() << endl;
        ret = EXIT_FAILURE;
    }
    return ret;
}
