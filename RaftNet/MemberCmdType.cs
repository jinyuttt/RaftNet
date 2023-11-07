using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaftNet
{
    public enum MemberCmdType
    {
        Add,
        Remove,
        Get,
        Response,
    }
}
