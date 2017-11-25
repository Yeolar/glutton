/*
 * Copyright (C) 2017, Yeolar
 */

using namespace rdd;

void iprint(char opcode, QItem *item, int xid)
{
  cout << "*************** start *********************" << endl;
  switch(opcode) {
  case Journal::CMD_ADD:
    cout << "opcode: CMD_ADD" << endl;
    cout << *item << endl;
    break;
  case Journal::CMD_REMOVE:
    cout << "opcode: CMD_REMOVE" << endl;
    break;
  case Journal::CMD_ADDX:
    cout << "opcode: CMD_ADDX" << endl;
    cout << *item << endl;
    break;
  case Journal::CMD_REMOVE_TENTATIVE:
    cout << "opcode: CMD_REMOVE_TENTATIVE" << endl;
    break;
  case Journal::CMD_SAVE_XID:
    cout << "opcode: CMD_SAVE_XID" << endl;
    cout << "XID: " << xid << endl;
    break;
  case Journal::CMD_UNREMOVE:
    cout << "opcode: CMD_UNREMOVE" << endl;
    cout << "XID: " << xid << endl;
    break;
  case Journal::CMD_CONFIRM_REMOVE:
    cout << "opcode: CMD_CONFIRM_REMOVE" << endl;
    cout << "XID: " << xid << endl;
    break;
  case Journal::CMD_ADD_XID:
    cout << "opcode: CMD_ADD_XID" << endl;
    cout << "XID: " << xid << endl;
    cout << *item << endl;
    break;
  default:
    break;
  }
  cout << "*************** end *********************" << endl;
}

int main(int argc, char *argv[])
{
  if (argc != 2) {
    cout << argv[0] << " <queue file>" << endl;
    assert(0);
  }
  Journal journal(argv[1], false);
  journal.replay(argv[1], iprint);
  exit(0);
}

