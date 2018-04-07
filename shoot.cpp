#include <iostream>
using namespace std;

int main()
{
    string st;
    int t,n,i,d,j,flg,sum=0,k,g;
    int l=st.length(),count;
    bool rt,r;
    cin>>t;
    for(n=1;n<=t;n++)
    {
        rt=false;
        r=false;
        cin>>d>>st;
        flg=1;
        count=0;
        l=st.length();
        int ar[l];
        sum=0;
        for(int j=0;j<l;j++)
        {
            if(st[j]=='C')
            {
                flg=flg*2;
                ar[j]=flg;
            }
            else
            {
                r=true;
                ar[j]=flg;
                sum=sum+flg;
            }
        }
        if(flg==1)
        {
            cout<<"Case #"<<n<<": IMPOSSIBLE"<<endl;
            continue;
        }
        if ((r==false) || (sum<=d))
        {
            cout<<"Case #"<<n<<": 0"<<endl;
            continue;
        }
        if (sum>d)
        {
            for (i=0;i<l;i++)
            {
                for (j=i;j<l-1;j++)
                {
                    if (st[j]=='C' && st[j+1]=='S')
                    {
                        st[j]='S';
                        st[j+1]='C';
                        if (i==0 && j==0)
                            ar[j]=1;
                        else
                            ar[j]=ar[j-1];
                        count++;
                        sum=0;
                        for (k=0;st[k];k++)
                        {
                            if(st[k]=='S')
                                sum=sum+ar[k];
                        }
                      //  cout<<endl<<"i am sum "<<sum<<" sum complete"<<endl;
                        if (sum<=d)
                        {
                            cout<<"Case #"<<n<<": "<<count<<endl;
                            goto A;
                            rt=true;
                            break;
                        }
                    }
                }
            }
            A: g=1;
        }
    }
    
    return 0;
}
