package account

import (
	"clover-server-engine/internal/domain/data"
)

// AccountKey 构造"绑在账号上的其他数据"的 data.Key。
//
// 账号本身的账号名/密码/渠道走结构化表（见 Store / ChannelStore）；
// 而"挂在账号上的业务数据"（背包/邮件/设置/角色...）应复用通用 data 三元键存储层，
// 归属实体统一用 OwnerAccount，避免账号表无限制膨胀。
//
// 例：账号 1001 的背包数据
//
//	dataStore.Save(ctx, account.AccountKey("1001", "bag"), blob)
//
// 等价于 data.Key{Owner: data.OwnerAccount, ID: "1001", Type: "bag"}。
func AccountKey(account, typ string) data.Key {
	return data.Key{Owner: data.OwnerAccount, ID: account, Type: typ}
}
